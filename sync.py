import os
import json
import re
import argparse
import sys
import requests
from urllib.parse import urljoin

def setCredentials(serverUrl, username, password):
    """
    Set the credentials for synchronization with an ODK server.
    
    Args:
        serverUrl (str): The URL of the ODK server
        username (str): The username for authentication
        password (str): The password for authentication
    
    Returns:
        None
    
    Raises:
        ValueError: If any of the required parameters are missing or invalid
    """
    # Check if any required parameters are missing
    if not all([username, password, serverUrl]):
        raise ValueError("Username, password, and serverUrl are required.")
    
    # Check that serverUrl is a non-empty string
    if not isinstance(serverUrl, str) or not serverUrl:
        raise ValueError("'serverUrl' must be a non-empty string.")
    
    # Clean the server URL (remove trailing slashes and /odktables part)
    serverUrl = re.sub(r'/odktables.*|/+$', '', serverUrl.lower())
    
    # Create credentials dictionary
    credentials = {
        "username": username,
        "password": password,
        "server_url": serverUrl
    }
    
    # Get the path to the credentials file
    credentials_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".sync_credentials")
    
    # Write credentials to file
    with open(credentials_file, 'w') as f:
        json.dump(credentials, f, indent=4)
    
    print("Credentials and server URL have been saved to .sync_credentials file.")

def getCredentials():
    """
    Retrieve the stored credentials.
    
    Returns:
        dict: A dictionary containing username, password, and server_url
        
    Raises:
        FileNotFoundError: If the credentials file doesn't exist
    """
    credentials_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".sync_credentials")
    
    if not os.path.exists(credentials_file):
        raise FileNotFoundError("Credentials file not found. Please set credentials first.")
    
    with open(credentials_file, 'r') as f:
        credentials = json.load(f)
    
    return credentials


def resetAuth():
    """
    Reset the authentication for the current session.
    """
    # In Python, we don't need to explicitly reset the connection
    # as each request creates a new connection by default
    pass


def handleResponseStatus(response):
    """
    Handle HTTP response status codes and raise appropriate exceptions.
    
    Args:
        response: The HTTP response object
        
    Raises:
        Exception: If the response status code is not 200
    """
    status = response.status_code
    
    if status == 200:
        return
    
    error_messages = {
        401: "Unauthorized: Check your credentials.",
        403: "Forbidden: You don't have permission to access this resource.",
        404: "Not Found: The requested resource was not found.",
        500: "Internal Server Error: The server encountered an error."
    }
    
    error_msg = error_messages.get(status, f"HTTP error {status}: {response.text}")
    raise Exception(error_msg)


def getResponse(urlSegment, writeToPath=None):
    """
    Make an HTTP GET request to the specified URL segment and handle the response.
    
    Args:
        urlSegment (str): The URL segment to append to the base URL
        writeToPath (str, optional): Path to write binary response content to
        
    Returns:
        dict or str: Parsed JSON response or path to written file
        
    Raises:
        Exception: If the request fails or returns an error status
    """
    creds = getCredentials()
    baseUrl = creds["server_url"]
    username = creds["username"]
    password = creds["password"]
    
    # Determine if the URL segment is a complete URL or just a path
    if urlSegment.startswith(baseUrl):
        url = urlSegment
    else:
        # Ensure the URL segment starts with a single slash
        urlSegment = "/" + urlSegment.lstrip("/")
        
        # Ensure the URL segment starts with /odktables/ if it doesn't already
        if not urlSegment.startswith("/odktables/"):
            urlSegment = "/odktables/" + urlSegment.lstrip("/")
        
        url = urljoin(baseUrl, urlSegment)
    
    try:
        response = requests.get(
            url,
            auth=(username, password),
            headers={
                "User-Agent": "python-odkx-client",
                "Content-Type": "application/json"
            }
        )
        
        handleResponseStatus(response)
        
        if writeToPath is None:
            return response.json()
        else:
            with open(writeToPath, 'wb') as f:
                f.write(response.content)
            return writeToPath
            
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data from: {url}\nError message: {str(e)}")
        raise Exception("Unable to connect to sync endpoint")


def checkAuth():
    """
    Verify if the user has sufficient permissions to download data.
    
    Returns:
        bool: True if the user has admin access, False otherwise
    """
    try:
        # Reset authentication (not strictly necessary in Python)
        resetAuth()
        
        # Get privileges information
        auth = getResponse("/odktables/default/privilegesInfo")
        
        # Check if the response contains roles
        if auth is None or not isinstance(auth, dict) or "roles" not in auth or auth["roles"] is None:
            return False
        
        # Check if the user has admin role
        admin_role = "ROLE_SITE_ACCESS_ADMIN"
        has_admin = admin_role in auth["roles"]
        
        if not has_admin:
            print(f"User is authenticated, but doesn't have the required permissions ({admin_role}).")
        
        return has_admin
        
    except Exception as e:
        print(f"Authentication check failed: {str(e)}")
        return False


def help():
    """
    Display help information about available commands.
    """
    print("\nODK-X Database Migration Tool - Sync Utilities\n")
    print("Available commands:")
    print("  setCredentials  - Set server credentials for synchronization")
    print("  checkAuth      - Verify if the user has sufficient permissions to download data")
    print("\nUsage examples:")
    print("  python sync.py setCredentials --server \"https://example.org\" --username \"user\" --password \"pass\"")
    print("  python sync.py checkAuth")
    print("  python sync.py help\n")


def main():
    """
    Main function to handle command-line arguments.
    """
    parser = argparse.ArgumentParser(description="ODK-X Database Migration Tool - Sync Utilities", add_help=False)
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # setCredentials command
    set_creds_parser = subparsers.add_parser("setCredentials", help="Set server credentials for synchronization")
    set_creds_parser.add_argument("--server", required=True, help="Server URL")
    set_creds_parser.add_argument("--username", required=True, help="Username for authentication")
    set_creds_parser.add_argument("--password", required=True, help="Password for authentication")
    
    # checkAuth command
    check_auth_parser = subparsers.add_parser("checkAuth", help="Verify if the user has sufficient permissions to download data")
    
    # help command
    help_parser = subparsers.add_parser("help", help="Show help information")
    
    # Parse arguments
    args = parser.parse_args()
    
    # Execute the appropriate command
    if args.command == "setCredentials":
        setCredentials(args.server, args.username, args.password)
    elif args.command == "checkAuth":
        has_access = checkAuth()
        if has_access:
            print("Authentication successful! User has admin access.")
        else:
            print("Authentication failed or insufficient permissions.")
    elif args.command == "help" or args.command is None:
        help()
    else:
        print(f"Unknown command: {args.command}")
        help()


if __name__ == "__main__":
    main()
