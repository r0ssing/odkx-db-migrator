#!/usr/bin/env python3
import sys
import os
import subprocess
import sqlite3
from typing import List, Dict, Set, Tuple
import argparse

def ensure_data_directory():
    """Ensure the data directory exists."""
    if not os.path.exists('data'):
        os.makedirs('data')

def pull_database(target_file=None):
    """Pull the ODK-X database from the connected Android device.
    
    Pulls the database file from the device at:
    /sdcard/opendatakit/default/data/webDb/sqlite.db
    
    And saves it locally to:
    data/target.db (default) or the specified target file
    
    Args:
        target_file: Optional. If provided, save the database to this filename
                    within the data directory. If None, defaults to 'target.db'.
    """
    ensure_data_directory()
    device_path = '/sdcard/opendatakit/default/data/webDb/sqlite.db'
    
    # Use the specified target file or default to 'target.db'
    if target_file:
        target_path = os.path.join('data', target_file)
    else:
        target_path = os.path.join('data', 'target.db')
    
    try:
        # Pull the database file from device
        subprocess.run(['adb', 'pull', device_path, target_path], check=True)
        print(f"Successfully pulled database to {target_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error pulling database: {e}")
        sys.exit(1)

def push_database():
    """Push the local database to the connected Android device.
    
    This function:
    1. Calls clean_device_db() to close ODK-X apps and remove existing database files
    2. Pushes the database file from the local path:
       data/target.db
    
    To the device at:
    /sdcard/opendatakit/default/data/webDb/sqlite.db
    """
    source_path = os.path.join('data', 'target.db')
    device_path = '/sdcard/opendatakit/default/data/webDb/sqlite.db'
    
    if not os.path.exists(source_path):
        print(f"Error: Source file {source_path} does not exist")
        sys.exit(1)
    
    try:
        # First, clean up the device database
        print("Cleaning up device database before pushing...")
        clean_device_db()
        
        # Push the database file to device
        print("Pushing database to device...")
        subprocess.run(['adb', 'push', source_path, device_path], check=True)
        print(f"Successfully pushed database to device at {device_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error pushing database: {e}")
        sys.exit(1)

def clean_device_db():
    """Clean up the ODK-X database and temporary files on the device.
    
    This function:
    1. Closes all ODK-X apps (Survey, Tables, Services)
    2. Removes the main SQLite database file
    3. Removes any lock/temp SQLite database files (.db-journal, .db-wal, .db-shm, etc.)
    from the device at:
    /sdcard/opendatakit/default/data/webDb/
    
    Returns:
        int: Number of files removed
    """
    device_dir = '/sdcard/opendatakit/default/data/webDb/'
    main_db_file = os.path.join(device_dir, 'sqlite.db')
    
    try:
        # First, close all ODK-X apps
        print("Closing ODK-X apps...")
        try:
            subprocess.run(['adb', 'shell', 'am', 'force-stop', 'org.opendatakit.survey'], check=True)
            subprocess.run(['adb', 'shell', 'am', 'force-stop', 'org.opendatakit.tables'], check=True)
            subprocess.run(['adb', 'shell', 'am', 'force-stop', 'org.opendatakit.services'], check=True)
            print("All ODK-X apps closed successfully")
        except subprocess.CalledProcessError as e:
            print(f"Warning: Error closing one or more ODK-X apps: {e}")
            print("Continuing with database cleanup...")
        
        # List all files in the directory
        result = subprocess.run(
            ['adb', 'shell', 'ls', '-la', device_dir], 
            check=True, 
            capture_output=True, 
            text=True
        )
        
        # Find all database and temporary SQLite files
        files_to_remove = []
        for line in result.stdout.splitlines():
            # Skip directory entries and non-file lines
            if line.startswith('d') or not line.strip():
                continue
                
            # Extract filename
            parts = line.split()
            if len(parts) >= 8:  # Standard ls -la format has at least 8 parts
                filename = parts[-1]
                file_path = os.path.join(device_dir, filename)
                
                # Add main database file
                if filename == 'sqlite.db':
                    files_to_remove.append(file_path)
                # Check for SQLite temp file patterns
                elif (filename.endswith('.db-journal') or 
                    filename.endswith('.db-wal') or 
                    filename.endswith('.db-shm') or 
                    filename.endswith('.db.was') or
                    '.db-' in filename):
                    files_to_remove.append(file_path)
        
        # Remove each file
        removed_count = 0
        for file_path in files_to_remove:
            try:
                print(f"Removing file: {file_path}")
                subprocess.run(['adb', 'shell', 'rm', file_path], check=True)
                removed_count += 1
            except subprocess.CalledProcessError as e:
                print(f"Error removing file {file_path}: {e}")
        
        if removed_count > 0:
            print(f"Successfully removed {removed_count} database files")
        else:
            print("No database files found to remove")
            
        return removed_count
        
    except subprocess.CalledProcessError as e:
        print(f"Error accessing device directory: {e}")
        sys.exit(1)

def push_attachments():
    """Push the attachment folders to the connected Android device.
    
    Copies all folders and files under data/attachments to the device at
    /sdcard/opendatakit/default/data/tables/[table_name]/instances/
    
    Raises:
        FileNotFoundError: If the attachments directory doesn't exist
        subprocess.CalledProcessError: If there's an error pushing the files
    """
    source_dir = os.path.join('data', 'attachments')
    device_base_dir = '/sdcard/opendatakit/default/data/tables'
    
    if not os.path.exists(source_dir):
        print(f"Error: Source directory {source_dir} does not exist")
        sys.exit(1)
    
    # Get a list of all tables with attachments
    tables = [d for d in os.listdir(source_dir) if os.path.isdir(os.path.join(source_dir, d))]
    
    if not tables:
        print(f"No attachment directories found in {source_dir}")
        return
    
    print(f"Pushing attachment folders to device...")
    print(f"Source: {source_dir}")
    print(f"Destination: {device_base_dir}")
    
    successful_tables = 0
    successful_instances = 0
    failed_tables = []
    
    for table in tables:
        source_table_dir = os.path.join(source_dir, table)
        device_table_dir = f"{device_base_dir}/{table}"
        
        print(f"\nProcessing table: {table}")
        
        try:
            # Create the table directory on the device if it doesn't exist
            subprocess.run(['adb', 'shell', 'mkdir', '-p', device_table_dir], check=True)
            
            # Get all instance directories in the source table directory
            instance_dirs = os.listdir(source_table_dir)
            if 'instances' in instance_dirs:
                # If there's an 'instances' directory, push it directly to the table directory
                source_instances_dir = os.path.join(source_table_dir, 'instances')
                device_instances_dir = f"{device_table_dir}/instances"
                
                # Create the instances directory on the device
                subprocess.run(['adb', 'shell', 'mkdir', '-p', device_instances_dir], check=True)
                
                table_instances = 0
                
                # Push all instance subdirectories
                for instance_dir in os.listdir(source_instances_dir):
                    source_instance_path = os.path.join(source_instances_dir, instance_dir)
                    device_instance_path = f"{device_instances_dir}/{instance_dir}"
                    
                    if os.path.isdir(source_instance_path):
                        try:
                            # Create the instance directory on the device
                            subprocess.run(['adb', 'shell', 'mkdir', '-p', device_instance_path], check=True)
                            
                            # Count files to be pushed
                            files = [f for f in os.listdir(source_instance_path) if os.path.isfile(os.path.join(source_instance_path, f))]
                            if files:
                                # Push all files in the instance directory
                                print(f"  Pushing {len(files)} files for instance {instance_dir}")
                                subprocess.run(['adb', 'push', f"{source_instance_path}/.", device_instance_path], check=True)
                                table_instances += 1
                                successful_instances += 1
                            else:
                                print(f"  No files found in instance {instance_dir}")
                        except subprocess.CalledProcessError as e:
                            print(f"  Error pushing instance {instance_dir}: {e}")
                
                print(f"Successfully pushed {table_instances} instances for table {table}")
                successful_tables += 1
            else:
                print(f"No 'instances' directory found in {source_table_dir}")
        except subprocess.CalledProcessError as e:
            print(f"Error processing table {table}: {e}")
            failed_tables.append(table)
            continue
    
    print("\n" + "="*50)
    print("Attachment Push Summary:")
    print(f"Successfully pushed {successful_tables} tables with {successful_instances} instances")
    
    if failed_tables:
        print(f"Failed to push {len(failed_tables)} tables: {', '.join(failed_tables)}")
    
    print("="*50)
    print("Finished pushing attachment folders to device")

def get_form_tables() -> List[str]:
    """Get all tables from target.db that have both _form_id and _row_etag columns.
    
    Returns:
        List[str]: A sorted list of table names that have the required columns.
        
    Raises:
        FileNotFoundError: If the database file doesn't exist
        sqlite3.Error: If there's an error accessing the database
    """
    db_path = os.path.join('data', 'target.db')
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file {db_path} does not exist")

    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        
        # Get all tables first
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        form_tables = []
        for (table_name,) in tables:
            # For each table, check if it has both required columns
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = {col[1] for col in cursor.fetchall()}
            if '_form_id' in columns and '_row_etag' in columns:
                form_tables.append(table_name)
        
        return sorted(form_tables)
    finally:
        conn.close()

def remove_instance_rows(table_name=None):
    """Remove all rows from form tables in the local target database (data/target.db).
    
    Args:
        table_name: Optional. If provided, only remove rows from this specific table.
                   If None, remove rows from all form tables.
    
    Raises:
        FileNotFoundError: If the database file doesn't exist
        sqlite3.Error: If there's an error accessing the database
        ValueError: If the specified table is not a valid form table
    """
    db_path = os.path.join('data', 'target.db')
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file {db_path} does not exist")

    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        all_form_tables = get_form_tables()
        
        # Determine which tables to process
        if table_name:
            # Check if the specified table is a valid form table
            if table_name not in all_form_tables:
                raise ValueError(f"Table '{table_name}' is not a valid form table")
            tables_to_process = [table_name]
        else:
            tables_to_process = all_form_tables
        
        # Start a transaction for atomicity
        cursor.execute("BEGIN TRANSACTION")
        
        total_rows_deleted = 0
        for table in tables_to_process:
            cursor.execute(f"DELETE FROM {table}")
            rows_deleted = cursor.rowcount
            total_rows_deleted += rows_deleted
            print(f"Deleted {rows_deleted} rows from {table}")
        
        # Commit all changes
        conn.commit()
        
        if table_name:
            print(f"\nSuccessfully removed {total_rows_deleted} rows from table '{table_name}'")
        else:
            print(f"\nSuccessfully removed {total_rows_deleted} rows from {len(tables_to_process)} form tables")
        
    except Exception as e:
        # Roll back on any error
        conn.rollback()
        raise
    finally:
        conn.close()

def show_form_tables():
    """Display all form tables from target.db."""
    try:
        form_tables = get_form_tables()
        if form_tables:
            print("\nForm tables found:")
            for table in form_tables:
                print(f"- {table}")
        else:
            print("\nNo form tables found")
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except sqlite3.Error as e:
        print(f"Error accessing database: {e}")
        sys.exit(1)

def scrub_sync_state():
    """Update sync state columns in all form tables to their default values.
    
    Updates the following columns in all form tables:
    - _conflict_type: null
    - _default_access: "FULL"
    - _row_etag: null
    - _sync_state: "new_row"
    
    Raises:
        FileNotFoundError: If the database file doesn't exist
        sqlite3.Error: If there's an error accessing the database
    """
    db_path = os.path.join('data', 'target.db')
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file {db_path} does not exist")

    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        form_tables = get_form_tables()
        
        # Start a transaction for atomicity
        cursor.execute("BEGIN TRANSACTION")
        
        for table in form_tables:
            # Update all sync-related columns
            cursor.execute(f"""
                UPDATE {table}
                SET _conflict_type = NULL,
                    _default_access = 'FULL',
                    _row_etag = NULL,
                    _sync_state = 'new_row'
            """)
            rows_updated = cursor.rowcount
            print(f"Updated {rows_updated} rows in {table}")
        
        # Commit all changes
        conn.commit()
        print("\nSuccessfully updated sync state in all form tables")
        
    except Exception as e:
        # Roll back on any error
        conn.rollback()
        raise
    finally:
        conn.close()

def get_forms_with_attachments() -> List[str]:
    """Get all form tables that have attachment columns (ending with _uriFragment).
    
    Returns:
        List[str]: A sorted list of table names that have both the required form columns
                  and at least one column ending with _uriFragment.
        
    Raises:
        FileNotFoundError: If the database file doesn't exist
        sqlite3.Error: If there's an error accessing the database
    """
    db_path = os.path.join('data', 'target.db')
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file {db_path} does not exist")

    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        
        # Get all tables first
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        form_tables_with_attachments = []
        for (table_name,) in tables:
            # For each table, check if it has both required columns
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = {col[1] for col in cursor.fetchall()}
            
            # Check if it's a form table first
            if '_form_id' in columns and '_row_etag' in columns:
                # Then check if it has any uriFragment columns
                if any(col.endswith('_uriFragment') for col in columns):
                    form_tables_with_attachments.append(table_name)
        
        return sorted(form_tables_with_attachments)
    finally:
        conn.close()

def get_uri_fragment_columns(cursor, table_name: str) -> List[str]:
    """Get all column names ending with _uriFragment from a table."""
    cursor.execute(f"PRAGMA table_info({table_name})")
    return [col[1] for col in cursor.fetchall() if col[1].endswith('_uriFragment')]

def get_expected_attachment_paths() -> Dict[str, Set[str]]:
    """Get all expected attachment paths from the database.
    
    Returns:
        Dict[str, Set[str]]: Dictionary mapping table names to sets of expected attachment paths
        
    Raises:
        FileNotFoundError: If the database file doesn't exist
        sqlite3.Error: If there's an error accessing the database
    """
    db_path = os.path.join('data', 'target.db')
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file {db_path} does not exist")

    conn = sqlite3.connect(db_path)
    expected_paths: Dict[str, Set[str]] = {}
    
    try:
        cursor = conn.cursor()
        tables = get_forms_with_attachments()
        
        for table in tables:
            # Get all uriFragment columns for this table
            uri_columns = get_uri_fragment_columns(cursor, table)
            table_paths = set()
            
            # Build a query that selects rows where any uriFragment column is not null
            uri_conditions = " OR ".join(f"{col} IS NOT NULL" for col in uri_columns)
            query = f"""
                SELECT _form_id, _id, {', '.join(uri_columns)}
                FROM {table}
                WHERE {uri_conditions}
            """
            
            cursor.execute(query)
            rows = cursor.fetchall()
            
            for row in rows:
                form_id = row[0]
                row_id = row[1].replace(':', '_').replace('-', '_')
                
                # Check each uriFragment column in this row
                for i, uri_column in enumerate(uri_columns, start=2):
                    uri_fragment = row[i]
                    if uri_fragment:  # Skip empty/null values
                        # Include 'instances' in the path to match the actual file system structure
                        expected_path = os.path.join('data', 'attachments', form_id, 'instances', row_id, uri_fragment)
                        table_paths.add(expected_path)
            
            if table_paths:
                expected_paths[table] = table_paths
    
    finally:
        conn.close()
    
    return expected_paths

def get_actual_attachment_paths() -> Set[str]:
    """Get all actual attachment files from the attachments directory.
    
    Returns:
        Set[str]: Set of paths to all files in the attachments directory
    """
    attachments_dir = os.path.join('data', 'attachments')
    if not os.path.exists(attachments_dir):
        return set()
        
    actual_paths = set()
    for root, _, files in os.walk(attachments_dir):
        for file in files:
            path = os.path.join(root, file)
            # Normalize path separators to match expected paths
            actual_paths.add(path.replace('\\', '/'))
    
    return actual_paths

def remove_empty_files(directory: str, verbose: bool = True) -> List[str]:
    """Remove all zero-byte files from a directory and its subdirectories.
    
    Args:
        directory: Directory to clean
        verbose: If True, print progress information
    
    Returns:
        List[str]: Paths of removed files
    """
    removed_files = []
    if not os.path.exists(directory):
        return removed_files
        
    for root, _, files in os.walk(directory):
        for file in files:
            path = os.path.join(root, file)
            if os.path.getsize(path) == 0:
                os.remove(path)
                removed_files.append(path)
                if verbose:
                    print(f"Removed empty file: {path}")
    
    return removed_files

def remove_empty_dirs(directory: str, verbose: bool = True) -> List[str]:
    """Remove empty directories recursively.
    
    Args:
        directory: Directory to clean
        verbose: If True, print progress information
    
    Returns:
        List[str]: Paths of removed directories
    """
    removed_dirs = []
    if not os.path.exists(directory):
        return removed_dirs
        
    for root, dirs, files in os.walk(directory, topdown=False):
        # Process directories bottom-up
        for dir_name in dirs:
            dir_path = os.path.join(root, dir_name)
            try:
                os.rmdir(dir_path)  # Will only succeed if directory is empty
                removed_dirs.append(dir_path)
                if verbose:
                    print(f"Removed empty directory: {dir_path}")
            except OSError:
                # Directory not empty, skip it
                pass
    
    return removed_dirs

def update_missing_attachment_refs(conn: sqlite3.Connection, table: str, missing_paths: List[str], verbose: bool = True) -> int:
    """Update database rows to clear references to missing attachments.
    
    Args:
        conn: Database connection
        table: Table name to update
        missing_paths: List of missing attachment paths
        verbose: If True, print progress information
    
    Returns:
        int: Number of rows updated
    """
    cursor = conn.cursor()
    updates = 0
    
    # Get all uriFragment columns and their matching contentType columns
    uri_columns = get_uri_fragment_columns(cursor, table)
    content_type_columns = [col.replace('_uriFragment', '_contentType') for col in uri_columns]
    
    # Build a query to find rows with these missing attachments
    for uri_column, content_type_column in zip(uri_columns, content_type_columns):
        for path in missing_paths:
            # Extract the uriFragment from the full path
            uri_fragment = os.path.basename(path)
            
            # Update the row, setting both uriFragment and contentType to NULL
            cursor.execute(f"""
                UPDATE {table}
                SET {uri_column} = NULL,
                    {content_type_column} = NULL
                WHERE {uri_column} = ?
            """, (uri_fragment,))
            
            if cursor.rowcount > 0:
                updates += cursor.rowcount
                if verbose:
                    print(f"Updated {cursor.rowcount} rows in {table} to remove reference to {uri_fragment}")
    
    return updates

def validate_attachments(verbose: bool = True, autofix: bool = False, table: str = None) -> Tuple[Dict[str, List[str]], List[str]]:
    """Validate attachments and identify orphaned files.
    
    Args:
        verbose: If True, print detailed progress information
        autofix: If True, automatically fix issues by:
                - Removing empty files
                - Removing orphaned files
                - Removing empty directories
                - Updating database to remove references to missing files
        table: If provided, only validate attachments for the specified table
    
    Returns:
        Tuple containing:
        - Dict[str, List[str]]: Dictionary mapping table names to lists of missing attachments
        - List[str]: List of orphaned attachment paths
        
    Raises:
        FileNotFoundError: If the database file doesn't exist
        sqlite3.Error: If there's an error accessing the database
    """
    attachments_dir = os.path.join('data', 'attachments')
    
    if autofix and verbose:
        print("\nPreparing attachments folder...")
        # If a table is specified, only clean that table's directory
        target_dir = attachments_dir
        if table:
            table_dir = os.path.join(attachments_dir, table)
            if os.path.exists(table_dir):
                target_dir = table_dir
        
        removed_files = remove_empty_files(target_dir, verbose)
        if removed_files:
            print(f"Removed {len(removed_files)} empty files")
    
    if verbose:
        print("\nCollecting expected attachment paths from database...")
    expected_paths_by_table = get_expected_attachment_paths()
    
    # Filter to only the specified table if provided
    if table:
        if table not in expected_paths_by_table:
            if verbose:
                print(f"Table '{table}' not found or has no attachments")
            return {}, []
        expected_paths_by_table = {table: expected_paths_by_table[table]}
    
    # Normalize all expected paths to use forward slashes for comparison
    all_expected_paths = {
        path.replace('\\', '/') 
        for paths in expected_paths_by_table.values() 
        for path in paths
    }
    
    if verbose:
        print("Scanning actual attachment files...")
    
    # If a table is specified, only scan that table's directory
    if table:
        table_dir = os.path.join(attachments_dir, table)
        actual_paths = set()
        if os.path.exists(table_dir):
            for root, _, files in os.walk(table_dir):
                for file in files:
                    path = os.path.join(root, file)
                    # Normalize path separators to match expected paths
                    actual_paths.add(path.replace('\\', '/'))
    else:
        actual_paths = get_actual_attachment_paths()
    
    # Find missing and orphaned files
    missing_files: Dict[str, List[str]] = {}
    for table_name, expected in expected_paths_by_table.items():
        if verbose:
            print(f"\nChecking attachments in table {table_name}...")
        
        # Normalize paths for comparison
        normalized_expected = [path.replace('\\', '/') for path in expected]
        
        table_missing = [
            path for path in normalized_expected 
            if not os.path.exists(path.replace('/', os.sep))
        ]
        
        if table_missing:
            missing_files[table_name] = table_missing
            if verbose:
                for path in table_missing:
                    print(f"Missing: {path}")
                print(f"Found {len(table_missing)} missing attachments in {table_name}")
        elif verbose:
            print(f"All attachments present in {table_name}")
    
    # Find orphaned files - only include files that don't match any expected path
    orphaned_files = sorted([
        path for path in actual_paths 
        if path not in all_expected_paths
    ])
    
    if autofix:
        if orphaned_files:
            if verbose:
                print("\nRemoving orphaned files...")
            for path in orphaned_files:
                try:
                    os.remove(path)
                    if verbose:
                        print(f"Removed orphaned file: {path}")
                except OSError as e:
                    print(f"Error removing {path}: {e}")
        
        if verbose:
            print("\nRemoving empty directories...")
        # If a table is specified, only clean that table's directory
        target_dir = attachments_dir
        if table:
            table_dir = os.path.join(attachments_dir, table)
            if os.path.exists(table_dir):
                target_dir = table_dir
        
        remove_empty_dirs(target_dir, verbose)
        
        if missing_files:
            if verbose:
                print("\nUpdating database to remove references to missing files...")
            db_path = os.path.join('data', 'target.db')
            conn = sqlite3.connect(db_path)
            try:
                conn.execute("BEGIN TRANSACTION")
                total_updates = 0
                
                for table_name, paths in missing_files.items():
                    updates = update_missing_attachment_refs(conn, table_name, paths, verbose)
                    total_updates += updates
                
                conn.commit()
                if verbose and total_updates:
                    print(f"\nUpdated {total_updates} rows to remove references to missing files")
            except Exception as e:
                conn.rollback()
                raise
            finally:
                conn.close()
    
    # Print summary
    if verbose:
        print("\nValidation Summary:")
        if missing_files:
            total_missing = sum(len(files) for files in missing_files.values())
            print(f"Found {total_missing} missing attachments across {len(missing_files)} tables")
        else:
            print("All expected attachments are present!")
            
        if orphaned_files:
            print(f"\nFound {len(orphaned_files)} orphaned files")
            if not autofix:
                for path in orphaned_files:
                    print(f"Orphaned: {path}")
        else:
            print("\nNo orphaned files found")
    
    return missing_files, orphaned_files

def show_forms_with_attachments():
    """Display all form tables that have attachment columns."""
    try:
        form_tables = get_forms_with_attachments()
        if form_tables:
            print("\nForm tables with attachments found:")
            for table in form_tables:
                print(f"- {table}")
        else:
            print("\nNo form tables with attachments found")
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except sqlite3.Error as e:
        print(f"Error accessing database: {e}")
        sys.exit(1)

def describe_table_changes(table_name: str):
    """
    Describe changes to a table based on the values in _column_definitions.
    
    This function compares the column definitions between source and target databases
    for a specific table and displays a summary of changes including:
    - Unchanged columns
    - Changed pseudotype columns
    - Dropped columns
    - New columns
    
    Args:
        table_name: The name of the table to analyze
        
    Raises:
        FileNotFoundError: If either database file doesn't exist
        sqlite3.Error: If there's an error accessing the databases
    """
    source_db_path = os.path.join('data', 'source.db')
    target_db_path = os.path.join('data', 'target.db')
    
    if not os.path.exists(source_db_path):
        raise FileNotFoundError(f"Source database file {source_db_path} does not exist")
    if not os.path.exists(target_db_path):
        raise FileNotFoundError(f"Target database file {target_db_path} does not exist")
    
    # Connect to both databases
    source_conn = sqlite3.connect(source_db_path)
    target_conn = sqlite3.connect(target_db_path)
    
    try:
        source_cursor = source_conn.cursor()
        target_cursor = target_conn.cursor()
        
        # Get column definitions from source database
        source_cursor.execute("""
            SELECT _element_key, _element_name, _element_type
            FROM _column_definitions
            WHERE _table_id = ?
            ORDER BY _element_key
        """, (table_name,))
        source_columns = {row[0]: {'name': row[1], 'type': row[2]} for row in source_cursor.fetchall()}
        
        # Get column definitions from target database
        target_cursor.execute("""
            SELECT _element_key, _element_name, _element_type
            FROM _column_definitions
            WHERE _table_id = ?
            ORDER BY _element_key
        """, (table_name,))
        target_columns = {row[0]: {'name': row[1], 'type': row[2]} for row in target_cursor.fetchall()}
        
        # Calculate changes
        unchanged_columns = []
        changed_pseudotype_columns = []
        dropped_columns = []
        new_columns = []
        
        # Find unchanged, changed pseudotype, and dropped columns
        for col_key, col_info in source_columns.items():
            if col_key in target_columns:
                if col_info['type'] == target_columns[col_key]['type']:
                    unchanged_columns.append(col_key)
                else:
                    changed_pseudotype_columns.append({
                        'column': col_key,
                        'source_type': col_info['type'],
                        'target_type': target_columns[col_key]['type']
                    })
            else:
                dropped_columns.append(col_key)
        
        # Find new columns
        for col_key in target_columns:
            if col_key not in source_columns:
                new_columns.append(col_key)
        
        # Display summary
        print(f"\nTable Changes Summary for '{table_name}':")
        print("=" * 50)
        
        print(f"{len(unchanged_columns)} columns unchanged.")
        
        if changed_pseudotype_columns:
            print(f"{len(changed_pseudotype_columns)} columns changed pseudo type:")
            for change in changed_pseudotype_columns:
                print(f"  - {change['column']}: {change['source_type']} -> {change['target_type']}")
        
        if dropped_columns:
            print(f"{len(dropped_columns)} columns dropped:")
            for col in dropped_columns:
                print(f"  - {col}")
        
        if new_columns:
            print(f"{len(new_columns)} new columns:")
            for col in new_columns:
                print(f"  - {col}")
        
        print("=" * 50)
        
    finally:
        source_conn.close()
        target_conn.close()

def help():
    """Display all available helper functions with their descriptions."""
    print("\nAvailable Helper Functions:")
    print("=" * 50)
    print("ensure_data_directory         - Ensure the data directory exists.")
    print("pull_database                 - Pull the ODK-X database from the connected Android device to data/target.db.")
    print("push_database                 - Push the local database (data/target.db) to the connected Android device.")
    print("clean_device_db               - Clean up the ODK-X database and temporary files on the device.")
    print("get_form_tables               - Get all tables from target.db that have both _form_id and _row_etag columns.")
    print("remove_instance_rows          - Remove all rows from form tables in the local target database (data/target.db).")
    print("show_form_tables              - Display all form tables from target.db.")
    print("scrub_sync_state              - Update sync state columns in all form tables to their default values.")
    print("get_forms_with_attachments    - Get all form tables that have attachment columns.")
    print("get_uri_fragment_columns      - Get all column names ending with _uriFragment from a table.")
    print("get_expected_attachment_paths - Get all expected attachment paths from the database.")
    print("get_actual_attachment_paths   - Get all actual attachment files from the attachments directory.")
    print("remove_empty_files            - Remove all zero-byte files from a directory and its subdirectories.")
    print("See also:")
    print("  Attachment resizing and analysis tools:")
    print("    python resize.py --help")
    print("    (resize_images, get_sizes, get_detailed_sizes)")
    print("remove_empty_dirs             - Remove empty directories recursively.")
    print("update_missing_attachment_refs - Update database rows to clear references to missing attachments.")
    print("validate_attachments          - Validate attachments and identify orphaned files.")
    print("show_forms_with_attachments   - Display all form tables that have attachment columns.")
    print("describe_table_changes        - Describe changes to a table based on column definitions comparison.")
    print("help                          - Display all available helper functions with their descriptions.")
    print("migrate                       - Run the database migration process.")
    
    print("\nCommand-line Usage:")
    print("=" * 50)
    print("  python helpers.py <command> [options]")
    
    print("\nAvailable commands:")
    print("  pull_database              - Pull the ODK-X database from connected device to data/target.db")
    print("                               Options: --file=<filename>")
    print("  push_database              - Push the local database (data/target.db) to connected device")
    print("  clean_device_db            - Clean up the ODK-X database and temporary files on the device")
    print("  push_attachments           - Push the attachment folders to connected device")
    print("  show_form_tables           - Show all form tables in the database")
    print("  remove_instance_rows       - Remove all rows from form tables in the local target database")
    print("                               Options: --table=<table_name>")
    print("  scrub_sync_state           - Reset sync state columns in all form tables")
    print("  show_forms_with_attachments - Show form tables that have attachment columns")
    print("  validate_attachments       - Check if all referenced attachments exist")
    print("                               Options: --table=<table_name>")
    print("  fix_attachments            - Validate and auto-fix attachment issues")
    print("                               Options: --table=<table_name>")
    print("  help                       - Display this help information")
    print("  execute_sql_source         - Execute SQL query against the source database")
    print("                               Options: --sql=<sql_query>")
    print("  execute_sql_target         - Execute SQL query against the target database")
    print("                               Options: --sql=<sql_query>")
    print("  migrate                    - Run the database migration process")
    print("                               Options: --table=<table_name> --verbose")
    print("  describe_table_changes     - Describe changes to a table based on column definitions comparison")
    print("                               Options: --table=<table_name>")

def execute_sql_source(sql: str, verbose: bool = True):
    """Execute SQL query against the source database.
    
    Args:
        sql: SQL query to execute
        verbose: If True, print detailed output
    
    Returns:
        List of rows returned by the query
        
    Raises:
        FileNotFoundError: If the database file doesn't exist
        sqlite3.Error: If there's an error executing the SQL
    """
    db_path = os.path.join('data', 'source.db')
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file {db_path} does not exist")
    
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        
        if verbose:
            print(f"Executing SQL on source database: {sql}")
        
        cursor.execute(sql)
        
        # Get column names
        column_names = [description[0] for description in cursor.description] if cursor.description else []
        
        # Fetch all rows
        rows = cursor.fetchall()
        
        if verbose:
            if not rows:
                print("No results returned")
            else:
                # Print column headers
                header = " | ".join(column_names)
                separator = "-" * len(header)
                print(f"\n{header}")
                print(separator)
                
                # Print rows
                for row in rows:
                    formatted_row = " | ".join(str(value) for value in row)
                    print(formatted_row)
                
                print(f"\nTotal rows: {len(rows)}")
        
        return rows
    
    finally:
        conn.close()

def execute_sql_target(sql: str, verbose: bool = True):
    """Execute SQL query against the target database.
    
    Args:
        sql: SQL query to execute
        verbose: If True, print detailed output
    
    Returns:
        List of rows returned by the query
        
    Raises:
        FileNotFoundError: If the database file doesn't exist
        sqlite3.Error: If there's an error executing the SQL
    """
    db_path = os.path.join('data', 'target.db')
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file {db_path} does not exist")
    
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        
        if verbose:
            print(f"Executing SQL on target database: {sql}")
        
        cursor.execute(sql)
        
        # Get column names
        column_names = [description[0] for description in cursor.description] if cursor.description else []
        
        # Fetch all rows
        rows = cursor.fetchall()
        
        if verbose:
            if not rows:
                print("No results returned")
            else:
                # Print column headers
                header = " | ".join(column_names)
                separator = "-" * len(header)
                print(f"\n{header}")
                print(separator)
                
                # Print rows
                for row in rows:
                    formatted_row = " | ".join(str(value) for value in row)
                    print(formatted_row)
                
                print(f"\nTotal rows: {len(rows)}")
        
        return rows
    
    finally:
        conn.close()

def migrate(table_name=None, verbose=False):
    """
    Run the database migration process.
    
    This function initializes the DatabaseMigrator with the schema configuration
    and runs the migration process for all tables or a specific table.
    
    Args:
        table_name: Optional. If provided, only migrate this specific table.
                   If None, migrate all tables.
        verbose: If True, show detailed log messages during migration.
    
    Raises:
        Exception: If the migration process fails
    """
    from src.migrator import DatabaseMigrator
    from config.schema_config import SCHEMA_CONFIG
    import logging
    from tqdm import tqdm
    import sys
    import os

    # Configure root logger
    root_logger = logging.getLogger()
    
    # Store original handlers and level
    original_handlers = root_logger.handlers.copy()
    original_level = root_logger.level
    
    # Set up logging based on verbose flag
    if verbose:
        # Configure for verbose output
        root_logger.setLevel(logging.INFO)
        # Ensure we have a console handler
        if not any(isinstance(h, logging.StreamHandler) for h in root_logger.handlers):
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(logging.Formatter('%(levelname)s:%(name)s: %(message)s'))
            root_logger.addHandler(console_handler)
    else:
        # For non-verbose mode, only show ERROR messages
        root_logger.setLevel(logging.ERROR)
    
    try:
        # Initialize migrator
        migrator = DatabaseMigrator(SCHEMA_CONFIG)
        
        # Set the verbose mode on the migrator
        migrator.verbose_mode = verbose
        
        # Run migration
        if table_name:
            # For single table migration
            if verbose:
                logging.info(f"Migrating table: {table_name}")
            
            # Get record count for the table
            cursor = migrator.source_db.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_records = cursor.fetchone()[0]
            
            if total_records > 0:
                # Create a progress bar
                with tqdm(total=total_records, desc=f"Migrating {table_name}", ncols=100,
                          bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as pbar:
                    # Store original stats
                    original_count = migrator.migration_stats["total_records_migrated"]
                    
                    # Temporarily silence all loggers during migration to avoid interference with progress bar
                    if verbose:
                        temp_level = root_logger.level
                        root_logger.setLevel(logging.ERROR)
                    
                    # Migrate the table
                    migrator.migrate_table(table_name)
                    
                    # Restore logger level if in verbose mode
                    if verbose:
                        root_logger.setLevel(temp_level)
                    
                    # Update progress bar
                    migrated_count = migrator.migration_stats["total_records_migrated"] - original_count
                    pbar.update(migrated_count)
            else:
                # If no records, just migrate normally
                migrator.migrate_table(table_name)
                
            # Show summary for single table migration
            print(f"\nMigration complete. {migrator.migration_stats['total_records_migrated']} records migrated.")
        else:
            # For migrate_all, temporarily enable INFO level to show table counts
            if not verbose:
                root_logger.setLevel(logging.INFO)
            
            # For migrate_all, we need to handle the migration ourselves to ensure the progress bar is displayed properly
            # First, show the table counts and get user confirmation
            migrator._print_table_counts("Before Migration")
            
            # Get table names from both databases
            source_tables = migrator.get_table_names(migrator.source_db)
            target_tables = migrator.get_table_names(migrator.target_db)
            
            # Find tables that exist in both databases
            common_tables = source_tables.intersection(target_tables)
            
            # Log table differences
            source_only = source_tables - target_tables
            target_only = target_tables - source_tables
            
            if source_only:
                logging.info(f"\nTables in source but not in target: {', '.join(source_only)}")
                migrator.migration_stats["source_only_tables"] = list(source_only)
            if target_only:
                logging.info(f"Tables in target but not in source: {', '.join(target_only)}")
                migrator.migration_stats["target_only_tables"] = list(target_only)
            
            # Create a single progress bar for all tables
            tables_to_migrate = list(common_tables)
            total_tables = len(tables_to_migrate)
            
            logging.info(f"\nMigrating {total_tables} tables...")
            
            # Count total records to migrate for better progress estimation
            total_records = 0
            table_record_counts = {}
            
            for table_name in tables_to_migrate:
                cursor = migrator.source_db.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                record_count = cursor.fetchone()[0]
                table_record_counts[table_name] = record_count
                total_records += record_count
            
            logging.info(f"Total records to migrate: {total_records}")
            
            # Get user confirmation
            try:
                input("\nPress [Enter] to continue or [CTRL]+C to abort...")
            except KeyboardInterrupt:
                logging.info("\nMigration aborted by user.")
                raise
            
            # Clear the console to make room for the progress bar
            os.system('cls' if os.name == 'nt' else 'clear')
            
            # Silence all loggers during migration to avoid interference with progress bar
            root_logger.setLevel(logging.ERROR)
            
            # Create overall progress bar
            with tqdm(total=total_records, desc="Total migration progress", ncols=100,
                      bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as overall_bar:
                records_migrated = 0
                
                # Migrate common tables
                for table_name in tables_to_migrate:
                    # Update progress bar description
                    overall_bar.set_description(f"Migrating {table_name} ({records_migrated}/{total_records} records)")
                    
                    # Store the original total_records_migrated count
                    original_count = migrator.migration_stats["total_records_migrated"]
                    
                    # Migrate the table
                    migrator.migrate_table(table_name)
                    
                    # Calculate how many records were actually migrated
                    new_count = migrator.migration_stats["total_records_migrated"]
                    records_migrated_for_table = new_count - original_count
                    
                    # Update the overall progress bar
                    overall_bar.update(records_migrated_for_table)
                    records_migrated += records_migrated_for_table
                    
                    # Update migration stats
                    migrator.migration_stats["tables_migrated"] += 1
            
            # Update village values in hh_person table
            migrator.update_person_villages()
            
            # Show summary
            if verbose:
                # Restore logger level for verbose output
                root_logger.setLevel(logging.INFO)
                migrator._log_summary()
                migrator._print_table_counts("After Migration")
            else:
                # In non-verbose mode, just print a simple summary
                print(f"\nMigration complete. {migrator.migration_stats['total_records_migrated']} records migrated across {migrator.migration_stats['tables_migrated']} tables.")
    except Exception as e:
        logging.error(f"Migration failed: {str(e)}")
        raise
    finally:
        # Always restore original logger configuration
        root_logger.handlers = original_handlers
        root_logger.setLevel(original_level)

def main():
    if len(sys.argv) < 2:
        help()
        return
    
    command = sys.argv[1]
    
    if command == "help":
        help()
    elif command == "ensure_data_directory":
        ensure_data_directory()
    elif command == "pull_database":
        # Parse arguments
        parser = argparse.ArgumentParser(description="Pull database from device")
        parser.add_argument("--file", type=str, help="Output filename (default: target.db)")
        args, _ = parser.parse_known_args(sys.argv[2:])
        
        pull_database(args.file)
    elif command == "push_database":
        push_database()
    
    elif command == "clean_device_db":
        clean_device_db()
    elif command == "clean_db_tempfiles":
        # For backward compatibility
        clean_device_db()
    
    elif command == "push_attachments":
        push_attachments()
    
    elif command == "show_form_tables":
        show_form_tables()
    
    elif command == "show_forms_with_attachments":
        show_forms_with_attachments()
    
    elif command == "remove_instance_rows":
        # Parse arguments
        parser = argparse.ArgumentParser(description="Remove instance rows")
        parser.add_argument("--table", type=str, help="Remove rows from a specific table")
        args, _ = parser.parse_known_args(sys.argv[2:])
        
        remove_instance_rows(table_name=args.table)
    
    elif command == "scrub_sync_state":
        scrub_sync_state()
    
    elif command == "validate_attachments":
        # Parse arguments
        parser = argparse.ArgumentParser(description="Validate attachments")
        parser.add_argument("--autofix", action="store_true", help="Automatically fix issues")
        parser.add_argument("--table", type=str, help="Validate attachments for a specific table")
        args, _ = parser.parse_known_args(sys.argv[2:])
        
        validate_attachments(autofix=args.autofix, table=args.table)
    
    elif command == "fix_attachments":
        # Parse arguments
        parser = argparse.ArgumentParser(description="Fix attachment issues")
        parser.add_argument("--table", type=str, help="Fix attachments for a specific table")
        args, _ = parser.parse_known_args(sys.argv[2:])
        
        validate_attachments(autofix=True, table=args.table)
    
    elif command == "execute_sql_source":
        # Parse arguments
        parser = argparse.ArgumentParser(description="Execute SQL on source database")
        parser.add_argument("--sql", type=str, required=True, help="SQL query to execute")
        args, _ = parser.parse_known_args(sys.argv[2:])
        
        execute_sql_source(args.sql)
    
    elif command == "execute_sql_target":
        # Parse arguments
        parser = argparse.ArgumentParser(description="Execute SQL on target database")
        parser.add_argument("--sql", type=str, required=True, help="SQL query to execute")
        args, _ = parser.parse_known_args(sys.argv[2:])
        
        execute_sql_target(args.sql)
    
    elif command == "migrate":
        # Parse arguments
        parser = argparse.ArgumentParser(description="Run the database migration process")
        parser.add_argument("--table", help="Specific table to migrate")
        parser.add_argument("--verbose", action="store_true", help="Show detailed log messages during migration")
        args, _ = parser.parse_known_args(sys.argv[2:])
        
        migrate(args.table, verbose=args.verbose)
    elif command == "describe_table_changes":
        # Parse arguments
        parser = argparse.ArgumentParser(description="Describe changes to a table based on column definitions")
        parser.add_argument("--table", required=True, help="Table name to analyze")
        args, _ = parser.parse_known_args(sys.argv[2:])
        
        describe_table_changes(args.table)
    
    else:
        print(f"Unknown command: {command}")
        print("Use 'help' to see available commands")

if __name__ == '__main__':
    main()
