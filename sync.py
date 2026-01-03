import os
import json
import re
import argparse
import sys
import requests
import mimetypes
from urllib.parse import urljoin
from pathlib import Path

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


def constructFileUrl(baseUrl, filePath, version="2"):
    """
    Construct the URL for a file on the ODK server.
    
    Args:
        baseUrl (str): The base URL of the ODK server
        filePath (str): The relative path to the file on the server
        version (str, optional): The ODK version. Defaults to "2".
        
    Returns:
        str: The complete URL to the file
    """
    # Normalize the file path
    filePath = filePath.replace('\\', '/')
    if filePath.startswith('/'):
        filePath = filePath[1:]
    
    # Construct the URL
    return f"{baseUrl}/odktables/default/files/{version}/{filePath}"


def determineContentType(filename):
    """
    Determine the content type (MIME type) of a file based on its extension.
    
    Args:
        filename (str): The name of the file
        
    Returns:
        str: The content type of the file
    """
    content_type, _ = mimetypes.guess_type(filename)
    if content_type is None:
        # Default to binary stream if type cannot be determined
        content_type = 'application/octet-stream'
    return content_type


def pushFile(localFilePaths, remoteFolder):
    """
    Upload one or more files to the ODK server.
    
    Args:
        localFilePaths (str): Comma-separated list of paths to local files to upload
        remoteFolder (str): Relative path on the server where the files will be stored
        
    Returns:
        dict: Dictionary mapping file paths to their HTTP response status codes
        
    Raises:
        FileNotFoundError: If a local file does not exist
        Exception: If there's an error during the upload process
    """
    # Split the comma-separated list of paths
    paths = [path.strip() for path in localFilePaths.split(',')]
    results = {}
    
    try:
        # Get credentials
        creds = getCredentials()
        baseUrl = creds["server_url"]
        username = creds["username"]
        password = creds["password"]
        
        # Fixed parameters
        appId = "default"
        version = "2"
        
        # Normalize remote path
        remoteFolder = remoteFolder.replace('\\', '/')
        if not remoteFolder.endswith('/'):
            remoteFolder += '/'
            
        print(f"Preparing to upload {len(paths)} file(s) to {remoteFolder}...")
        
        for localFilePath in paths:
            try:
                # Normalize file path
                localFilePath = os.path.abspath(localFilePath)
                file = Path(localFilePath)
                
                # Check if file exists
                if not file.exists():
                    print(f"File {localFilePath} does not exist, skipping...")
                    results[localFilePath] = -1  # Use -1 to indicate an error
                    continue
                    
                # Check file size
                file_size = os.path.getsize(file)
                if file_size == 0:
                    print(f"File {localFilePath} has 0KB size. The API does not support uploading empty files, skipping...")
                    results[localFilePath] = -2  # Use -2 to indicate a size error
                    continue
                
                # Read file data
                with open(file, 'rb') as f:
                    data = f.read()
                
                # Construct the URI for file upload
                filename = os.path.basename(localFilePath)
                relativePathOnServer = f"{remoteFolder}{filename}"
                uploadUri = constructFileUrl(baseUrl, relativePathOnServer)
                
                print(f"Uploading file to: {uploadUri}")
                
                # Determine content type
                contentType = determineContentType(filename)
                
                # Make the POST request
                response = requests.post(
                    uploadUri,
                    data=data,
                    auth=(username, password),
                    headers={
                        "User-Agent": "python-odkx-client",
                        "Content-Type": contentType
                    }
                )
                
                # Print response details
                status_message = f"{response.status_code} (created)" if response.status_code == 201 else f"{response.status_code}"
                print(f"Upload response status code: {status_message}")
                
                if response.text:
                    print(f"Response content: {response.text}")
                
                # Store the result
                results[localFilePath] = response.status_code
                
            except Exception as e:
                print(f"Failed to upload {localFilePath}: {str(e)}")
                results[localFilePath] = -3  # Use -3 to indicate a general error
        
        # Print summary
        print("\nUpload Summary:")
        successful = sum(1 for code in results.values() if code in [200, 201])
        print(f"Successfully uploaded: {successful}/{len(paths)} files")
        
        return results
        
    except Exception as e:
        print(f"Upload process failed: {str(e)}")
        raise


def deleteFile(filePaths):
    """
    Delete one or more files from the ODK server.
    
    Args:
        filePaths (str): Comma-separated list of relative paths to files on the server
        
    Returns:
        dict: Dictionary mapping file paths to their HTTP response status codes
        
    Raises:
        Exception: If there's an error during the deletion process
    """
    # Split the comma-separated list of paths
    paths = [path.strip() for path in filePaths.split(',')]
    results = {}
    
    try:
        # Get credentials
        creds = getCredentials()
        baseUrl = creds["server_url"]
        username = creds["username"]
        password = creds["password"]
        
        print(f"Preparing to delete {len(paths)} file(s)...")
        
        for filePath in paths:
            try:
                # Construct the URI for file deletion
                deleteUri = constructFileUrl(baseUrl, filePath)
                
                print(f"Deleting file at: {deleteUri}")
                
                # Make the DELETE request
                response = requests.delete(
                    deleteUri,
                    auth=(username, password),
                    headers={
                        "User-Agent": "python-odkx-client"
                    }
                )
                
                # Print response details
                status_message = f"{response.status_code}" 
                if response.status_code == 204:
                    status_message += " (no content)"
                print(f"Delete response status code: {status_message}")
                
                if response.text:
                    print(f"Response content: {response.text}")
                
                # Store the result
                results[filePath] = response.status_code
                
            except Exception as e:
                print(f"Failed to delete {filePath}: {str(e)}")
                results[filePath] = -1  # Use -1 to indicate an error
        
        # Print summary
        print("\nDeletion Summary:")
        successful = sum(1 for code in results.values() if code in [200, 204])
        print(f"Successfully deleted: {successful}/{len(paths)} files")
        
        return results
        
    except Exception as e:
        print(f"Deletion process failed: {str(e)}")
        raise

def listServerTables():
    tables = getServerTables()
    for table in tables:
        print(table)

def getServerTables():
    """
    Retrieve and display the list of tables from the ODK-X server.
    
    Args:
        version (str, optional): The ODK version. Defaults to "2".

    Returns:
        list: The list of table names or None if the request fails
    """
    creds = getCredentials()
    baseUrl = creds["server_url"]
    tableResponse = getResponse(f"{baseUrl}/odktables/default/tables/")
    tables = tableResponse["tables"]
    table_ids = [table["tableId"] for table in tables]
    return table_ids

def listTableFiles(version="2", tableName=""):
    """
    Retrieve and display the list of app files from the ODK-X server.
    
    Args:
        version (str, optional): The ODK version. Defaults to "2".
        tableName (str, optional): The name of the table to retrieve. 
                                 If not specified, the files are retrieved for all tables.

    Returns:
        dict: The manifest of app files or None if the request fails
    """
    creds = getCredentials()
    baseUrl = creds["server_url"]
    
    if not tableName:
        # Get all tables and their files
        tables = getServerTables()
        if not tables:
            print("No tables found.")
            return None


        all_files = []
        table_files = {}
        
        # First collect all files
        for table in tables:
            manifest = getFiles(version=version, 
                             manifestUrl=f"{baseUrl}/odktables/default/manifest/{version}/{table}")
            if manifest and "files" in manifest and manifest["files"]:
                # Add table name to each file for reference
                for file_info in manifest["files"]:
                    file_info["table"] = table
                all_files.extend(manifest["files"])
                table_files[table] = manifest
        
        if not all_files:
            print("No files found in any table.")
            return None
            
        # Print all files together with table information
        print("\n=== All Files ===")
        printFiles(all_files)
        
        return table_files
    else:
        # Single table case
        manifest = getFiles(version=version, 
                         manifestUrl=f"{baseUrl}/odktables/default/manifest/{version}/{tableName}")
        if manifest:
            listFiles(version=version, manifestUrl=f"{baseUrl}/odktables/default/manifest/{version}/{tableName}")
        return manifest


def listAppFiles(version="2"):
    # Get credentials
    creds = getCredentials()
    baseUrl = creds["server_url"]
    manifest = listFiles(version="2", manifestUrl=f"{baseUrl}/odktables/default/manifest/{version}/")
    return manifest

def getFiles(version="2", manifestUrl=""):
    """
    Retrieve the list of app files from the ODK-X server.
    
    Args:
        version (str, optional): The ODK version. Defaults to "2".
        manifestUrl (str, optional): The URL of the manifest to retrieve. 
                                  If not specified, all app files are retrieved.

    Returns:
        dict: The manifest of app files or None if the request fails
    """
    try:
        # Get credentials
        creds = getCredentials()
        if not manifestUrl:
            baseUrl = creds["server_url"]
            manifestUrl = f"{baseUrl}/odktables/default/manifest/{version}/"
        
        try:
            # Get the manifest
            manifest = getResponse(manifestUrl)
            
            # Process the manifest
            if manifest and isinstance(manifest, dict) and "files" in manifest and isinstance(manifest["files"], list):
                return manifest
            else:
                print("No manifest data received or invalid format.")
                return None
                
        except Exception as e:
            print(f"Error accessing manifest: {str(e)}")
            print("\nAlternative approach: You can try accessing the files directly using the pushFile and deleteFile commands")
            print("if you know the specific file paths on the server.")
            return None
        
    except Exception as e:
        print(f"Failed to retrieve app files manifest: {str(e)}")
        return None

def printFiles(files, title=None):
    """
    Print a formatted list of files.
    
    Args:
        files (list): List of file dictionaries
        title (str, optional): Optional title to display above the file list
    """
    if not files or not isinstance(files, list):
        print("No files to display.")
        return
    
    # Sort files alphabetically by filename
    sorted_files = sorted(files, key=lambda x: x.get("filename", ""))
    
    # Calculate column widths for better formatting
    filename_width = max(len(file.get("filename", "")) for file in sorted_files) + 2
    size_width = 12  # Enough for formatted file sizes
    
    # Print header
    if title:
        print(f"\n{title}")
    print("File List (sorted alphabetically):")
    print(f"{'File Path':{filename_width}} {'Size':{size_width}} {'Download URL'}")
    print("-" * (filename_width + size_width + 50))
    
    # Print each file
    for file in sorted_files:
        filename = file.get("filename", "")
        content_length = file.get("contentLength", 0)
        download_url = file.get("downloadUrl", "")
        
        # Format the size (convert bytes to KB, MB, etc.)
        if content_length < 1024:
            size_str = f"{content_length} B"
        elif content_length < 1024 * 1024:
            size_str = f"{content_length/1024:.1f} KB"
        else:
            size_str = f"{content_length/(1024*1024):.1f} MB"
        
        print(f"{filename:{filename_width}} {size_str:{size_width}} {download_url}")
    
    print(f"\nTotal files: {len(files)}")

def listFiles(version="2", manifestUrl=""):
    """
    Retrieve and display the list of app files from the ODK-X server.
    
    Args:
        version (str, optional): The ODK version. Defaults to "2".
        manifestUrl (str, optional): The URL of the manifest to display.
                                  If not specified, all app files are retrieved.

    Returns:
        dict: The manifest of app files or None if the request fails
    """
    manifest = getFiles(version, manifestUrl)
    
    if manifest and "files" in manifest and isinstance(manifest["files"], list):
        printFiles(manifest["files"])
        return manifest
    return None


def updateCoreAppFiles(distFolder):
    dist_path = Path(distFolder)
    if not dist_path.exists() or not dist_path.is_dir():
        raise FileNotFoundError(f"distFolder does not exist or is not a directory: {dist_path}")

    index_html = dist_path / "index.html"
    if not index_html.exists() or not index_html.is_file():
        raise FileNotFoundError(f"Missing required file: {index_html}")

    asset_dir = dist_path / "asset"
    assets_dir = dist_path / "assets"
    if asset_dir.exists() and asset_dir.is_dir():
        chosen_assets_dir = asset_dir
    elif assets_dir.exists() and assets_dir.is_dir():
        chosen_assets_dir = assets_dir
    else:
        raise FileNotFoundError(f"Missing required directory: {asset_dir} (or {assets_dir})")

    js_files = sorted(chosen_assets_dir.glob("index-*.js"))
    js_map_files = sorted(chosen_assets_dir.glob("index-*.js.map"))
    css_files = sorted(chosen_assets_dir.glob("index-*.css"))

    if len(js_files) != 1:
        raise FileNotFoundError(f"Expected exactly 1 index-*.js in {chosen_assets_dir}, found {len(js_files)}")
    if len(js_map_files) != 1:
        raise FileNotFoundError(f"Expected exactly 1 index-*.js.map in {chosen_assets_dir}, found {len(js_map_files)}")
    if len(css_files) != 1:
        raise FileNotFoundError(f"Expected exactly 1 index-*.css in {chosen_assets_dir}, found {len(css_files)}")

    local_js = js_files[0]
    local_js_map = js_map_files[0]
    local_css = css_files[0]

    manifest = getFiles(version="2")
    remote_delete_paths = ["assets/dist/index.html"]
    if manifest and isinstance(manifest, dict) and isinstance(manifest.get("files"), list):
        pattern = re.compile(r"^assets/dist/assets/index-.*\.(css|js|js\.map)$")
        for file_info in manifest["files"]:
            filename = file_info.get("filename", "")
            if pattern.match(filename):
                remote_delete_paths.append(filename)
    else:
        print("Warning: could not retrieve app files manifest; will only delete assets/dist/index.html")

    unique_remote_delete_paths = []
    seen = set()
    for p in remote_delete_paths:
        if p not in seen:
            unique_remote_delete_paths.append(p)
            seen.add(p)

    print("Deleting existing core app dist files from server...")
    deleteFile(", ".join(unique_remote_delete_paths))

    print("Uploading new core app dist files to server...")
    pushFile(str(index_html), "assets/dist/")
    pushFile(", ".join([str(local_css), str(local_js), str(local_js_map)]), "assets/dist/assets/")


def checkAuth():
    """
    Verify if the user has sufficient permissions to download data.
    
    Returns:
        bool: True if the user has admin access, False otherwise
    """
    try:
        
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
    print("  listAppFiles   - Retrieve and display the list of app files from the ODK-X server")
    print("  listServerTables - Retrieve and display the list of tables from the ODK-X server")
    print("  listTableFiles - Retrieve and display the list of table files from the ODK-X server")
    print("  pushFile       - Upload a file to the ODK server")
    print("  deleteFile     - Delete a file from the ODK server")
    print("  updateCoreAppFiles - Removes dist/index.html and associated js/css files, then uploads the new html/js/css files from the local dist folder")
    print("\nUsage examples:")
    print("  python sync.py setCredentials --server \"https://example.org\" --username \"user\" --password \"pass\"")
    print("  python sync.py checkAuth")
    print("  python sync.py listAppFiles")
    print("  python sync.py listServerTables")
    print("  python sync.py listTableFiles [--tableName tableId]")
    print("  python sync.py pushFile --path \"path/to/file1.html, path/to/file2.css\" --remoteFolder \"assets/dist/\"")
    print("  python sync.py deleteFile --path \"assets/dist/index.html, assets/dist/style.css\"")
    print("  python sync.py updateCoreAppFiles --distFolder \"./app-designer/dist/\"")
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
    
    # listAppFiles command
    list_app_files_parser = subparsers.add_parser("listAppFiles", help="Retrieve and display the list of app files from the ODK-X server")
    
    # listServerTables command
    list_server_tables_parser = subparsers.add_parser("listServerTables", help="Retrieve and display the list of tables from the ODK-X server")
    
    # listTableFiles command
    list_table_files_parser = subparsers.add_parser("listTableFiles", help="Retrieve and display the list of table files from the ODK-X server")
    list_table_files_parser.add_argument("--tableName", required=False, help="Name of the table to retrieve files for")
    
    # pushFile command
    push_file_parser = subparsers.add_parser("pushFile", help="Upload one or more files to the ODK server")
    push_file_parser.add_argument("--path", required=True, help="Comma-separated list of paths to local files to upload")
    push_file_parser.add_argument("--remoteFolder", required=True, help="Relative path on the server where the files will be stored")
    
    # deleteFile command
    delete_file_parser = subparsers.add_parser("deleteFile", help="Delete a file from the ODK server")
    delete_file_parser.add_argument("--path", required=True, help="Relative path to the file on the server")

    # updateCoreAppFiles command
    update_core_app_files_parser = subparsers.add_parser(
        "updateCoreAppFiles",
        help="Removes dist/index.html and associated js/css files, then uploads the new html/js/css files from the local dist folder"
    )
    update_core_app_files_parser.add_argument("--distFolder", required=True, help="Path to local dist folder containing index.html and assets/")
    
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
    elif args.command == "listAppFiles":
        try:
            listAppFiles()
        except Exception as e:
            print(f"Error: {str(e)}")
            sys.exit(1)
    elif args.command == "listTableFiles":
        try:
            listTableFiles(tableName=args.tableName)
        except Exception as e:
            print(f"Error: {str(e)}")
            sys.exit(1)
    elif args.command == "listServerTables":
        try:
            listServerTables()
        except Exception as e:
            print(f"Error: {str(e)}")
            sys.exit(1)
    elif args.command == "pushFile":
        try:
            results = pushFile(args.path, args.remoteFolder)
            # Summary is already printed by the pushFile function
        except Exception as e:
            print(f"Error: {str(e)}")
            sys.exit(1)
    elif args.command == "deleteFile":
        try:
            results = deleteFile(args.path)
            # Summary is already printed by the deleteFile function
        except Exception as e:
            print(f"Error: {str(e)}")
            sys.exit(1)
    elif args.command == "updateCoreAppFiles":
        try:
            updateCoreAppFiles(args.distFolder)
        except Exception as e:
            print(f"Error: {str(e)}")
            sys.exit(1)
    elif args.command == "help" or args.command is None:
        help()
    else:
        print(f"Unknown command: {args.command}")
        help()


if __name__ == "__main__":
    main()
