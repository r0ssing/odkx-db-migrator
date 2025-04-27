from sqlite3 import connect
from typing import List, Dict, Any, Set, Tuple, Callable, Type, Optional
from datetime import datetime, date
import logging
import json  # Add json import for array/string conversions
from src.utils import progress_bar_iter
from tqdm import tqdm

from src.models import DatabaseConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseMigrator:
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.source_db = connect(config.source_db_path)
        self.target_db = connect(config.target_db_path)
        # Flag to control verbose output
        self.verbose_mode = True
        self.migration_stats = {
            "tables_migrated": 0,
            "tables_skipped": 0,
            "total_records_migrated": 0,
            "source_only_tables": [],
            "target_only_tables": [],
            "source_only_columns": {},
            "target_only_columns": {},
            "type_conversion_issues": {},
            "pseudotype_conversions": {}
        }
        self.table_counts = {
            "before": {},
            "after": {}
        }
    
    def get_table_names(self, db) -> Set[str]:
        """Get all table names from a database, excluding those starting with '_' or 'L__'."""
        cursor = db.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return {table[0] for table in cursor.fetchall() 
                if not (table[0].startswith('_') or table[0].startswith('L__'))}
    
    def get_column_names(self, db, table_name: str) -> Set[str]:
        """Get all column names from a table."""
        cursor = db.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        return {col[1] for col in cursor.fetchall()}
    
    def get_column_type_info(self, db, table_name: str, column_name: str) -> Dict[str, Any]:
        """Get detailed type information for a column."""
        cursor = db.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        for col in cursor.fetchall():
            if col[1] == column_name:
                return {
                    'type': col[2],  # SQLite type name
                    'nullable': not col[3],  # NOT NULL constraint
                    'default': col[4],  # DEFAULT value
                    'primary_key': bool(col[5])  # PRIMARY KEY
                }
        return None
    
    def get_column_pseudotype(self, db, table_name: str) -> Dict[str, str]:
        """
        Get the pseudotype information for columns in a table from _column_definitions.
        
        Returns:
            Dict[str, str]: Dictionary mapping column names to their pseudotypes
        """
        cursor = db.cursor()
        try:
            # Query the _column_definitions table to get pseudotypes
            query = """
                SELECT _element_key, _element_type 
                FROM _column_definitions 
                WHERE _table_id = ?
            """
            cursor.execute(query, (table_name,))
            return {row[0]: row[1] for row in cursor.fetchall()}
        except Exception as e:
            logger.warning(f"Error getting pseudotypes for table {table_name}: {str(e)}")
            return {}
    
    def convert_value_by_pseudotype(self, value: Any, source_type: str, target_type: str, column_name: str) -> Any:
        """
        Convert a value based on source and target pseudotypes.
        
        Args:
            value: The value to convert
            source_type: The pseudotype in the source database
            target_type: The pseudotype in the target database
            column_name: The name of the column (for logging)
            
        Returns:
            The converted value
        """
        if value is None:
            return None
            
        # Handle string to array conversion
        if source_type == "string" and target_type == "array":
            try:
                # Check if the value is already a JSON string
                if value.startswith('[') and value.endswith(']'):
                    try:
                        # Try to parse it as JSON
                        return value
                    except:
                        pass
                
                # Wrap the string in a single-element array
                return json.dumps([value])
            except Exception as e:
                logger.warning(f"Error converting string to array for column {column_name}: {str(e)}")
                return json.dumps([str(value)])
        
        # Handle integer to array conversion
        if source_type == "integer" and target_type == "array":
            try:
                # Convert the integer to a string and wrap in an array
                return json.dumps([str(value)])
            except Exception as e:
                logger.warning(f"Error converting integer to array for column {column_name}: {str(e)}")
                return json.dumps(["0"])  # Default to ["0"] if conversion fails
                
        # Handle array to string conversion (take first element if it exists)
        if source_type == "array" and target_type == "string":
            try:
                # Try to parse the array
                if isinstance(value, str) and value.startswith('[') and value.endswith(']'):
                    array_value = json.loads(value)
                    if array_value and len(array_value) > 0:
                        return array_value[0]
                    return ""
                return str(value)
            except Exception as e:
                logger.warning(f"Error converting array to string for column {column_name}: {str(e)}")
                return str(value)
        
        # Log warning for unsupported pseudotype conversions
        if source_type != target_type:
            logger.warning(f"Unsupported pseudotype conversion for column {column_name}: {source_type} -> {target_type}")
            # Add to migration stats for reporting
            if "unsupported_conversions" not in self.migration_stats:
                self.migration_stats["unsupported_conversions"] = {}
            
            table_name = getattr(self, "_current_table", "unknown_table")
            if table_name not in self.migration_stats["unsupported_conversions"]:
                self.migration_stats["unsupported_conversions"][table_name] = {}
            
            if column_name not in self.migration_stats["unsupported_conversions"][table_name]:
                self.migration_stats["unsupported_conversions"][table_name][column_name] = {
                    "source_type": source_type,
                    "target_type": target_type,
                    "example_value": str(value)[:100]  # Truncate long values
                }
                
        # Default: return the original value
        return value
    
    def migrate_table(self, table_name: str):
        """Migrate a single table, handling column matching and transformations."""
        # Get the root logger to control all logging
        root_logger = logging.getLogger()
        src_logger = logging.getLogger("src.migrator")
        
        if root_logger.level <= logging.INFO:
            src_logger.info(f"\nProcessing table: {table_name}")
        
        # Store current table name for reference in other methods
        self._current_table = table_name
        
        # Check if source table has any rows
        cursor = self.source_db.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        source_count = cursor.fetchone()[0]
        if source_count == 0:
            if root_logger.level <= logging.INFO:
                src_logger.info(f"Skipping table {table_name} - no records in source")
            self.migration_stats["tables_skipped"] += 1
            return
        
        # Get column names from both databases
        source_columns = self.get_column_names(self.source_db, table_name)
        target_columns = self.get_column_names(self.target_db, table_name)
        
        # Get pseudotype information for source and target columns
        source_pseudotypes = self.get_column_pseudotype(self.source_db, table_name)
        target_pseudotypes = self.get_column_pseudotype(self.target_db, table_name)
        
        # Track pseudotype conversions for this table
        pseudotype_conversions = {}
        
        # Find matching and non-matching columns
        matching_columns = source_columns.intersection(target_columns)
        source_only = source_columns - target_columns
        target_only = target_columns - source_columns
        
        # Log column differences
        if source_only and logger.level <= logging.INFO:
            logger.info(f"Columns in source but not in target: {', '.join(source_only)}")
            self.migration_stats["source_only_columns"][table_name] = source_only
        if target_only and logger.level <= logging.INFO:
            logger.info(f"Columns in target but not in source: {', '.join(target_only)}")
            self.migration_stats["target_only_columns"][table_name] = target_only
        
        if not matching_columns:
            logger.warning(f"No matching columns found for table {table_name}")
            self.migration_stats["tables_skipped"] += 1
            return
        
        try:
            # Fetch records from source with limit if in test mode
            limit_clause = f""
            cursor.execute(f"SELECT * FROM {table_name} {limit_clause}")
            source_records = cursor.fetchall()
            
            # Get column names in the correct order
            cursor.execute(f"PRAGMA table_info({table_name})")
            column_info = cursor.fetchall()
            column_names = [col[1] for col in column_info]
            
            # Transform and insert records
            if source_records:
                logger.info(f"Attempting to insert {len(source_records)} records into {table_name}")
                try:
                    # Log first record for debugging
                    logger.info(f"Sample record: {dict(zip(column_names, source_records[0]))}")
                    
                    # Insert records in batches of 100 to avoid SQLite variable limit
                    batch_size = 100  # Reduced from 500 to stay under SQLite's variable limit
                    num_batches = (len(source_records) + batch_size - 1) // batch_size
                    
                    total_inserted = 0
                    total_errors = 0
                    
                    # Process each batch without individual progress bars
                    for i in range(0, len(source_records), batch_size):
                        batch = source_records[i:i + batch_size]
                        placeholders = ", ".join(["(" + ", ".join(["?" for _ in matching_columns]) + ")" for _ in batch])
                        insert_sql = f"INSERT INTO {table_name} ({', '.join(matching_columns)}) VALUES {placeholders}"
                        values = []
                        for record in batch:
                            record_dict = dict(zip(column_names, record))
                            for col in matching_columns:
                                source_type = source_pseudotypes.get(col, "string")
                                target_type = target_pseudotypes.get(col, "string")
                                if source_type != target_type:
                                    original_value = record_dict[col]
                                    converted_value = self.convert_value_by_pseudotype(
                                        original_value, source_type, target_type, col
                                    )
                                    record_dict[col] = converted_value
                            values.extend([record_dict[col] for col in matching_columns])
                        try:
                            # Use connection's execute method for proper transaction handling
                            target_cursor = self.target_db.cursor()
                            target_cursor.execute("BEGIN TRANSACTION")
                            target_cursor.execute(insert_sql, values)
                            self.target_db.commit()
                            total_inserted += len(batch)
                            # Log batch progress in a simple format without progress bar
                            logger.debug(f"Batch {i//batch_size + 1}/{num_batches}: Inserted {len(batch)} records")
                        except Exception as e:
                            # Properly handle rollback
                            try:
                                self.target_db.rollback()
                            except Exception as rollback_error:
                                logger.error(f"Rollback error: {str(rollback_error)}")
                            
                            total_errors += len(batch)
                            logger.error(f"Error inserting batch {i//batch_size + 1}/{num_batches}: {str(e)}")
                    
                    # Log pseudotype conversions if any occurred
                    if pseudotype_conversions:
                        self.migration_stats["pseudotype_conversions"][table_name] = pseudotype_conversions
                        logger.info(f"Applied pseudotype conversions for table {table_name}:")
                        for col, info in pseudotype_conversions.items():
                            logger.info(f"  Column {col}: {info['source_type']} -> {info['target_type']}")
                            for example in info["examples"]:
                                logger.info(f"    Example: {example['original']} -> {example['converted']}")
                    
                    self.migration_stats["total_records_migrated"] += total_inserted
                    logger.info(f"Successfully migrated {total_inserted} records with {total_errors} errors")
                except Exception as e:
                    logger.error(f"Error during migration for table {table_name}: {str(e)}")
                    raise
            else:
                logger.warning(f"No records to migrate for table {table_name}")
            
        except Exception as e:
            self.target_db.rollback()
            logger.error(f"Error migrating table {table_name}: {str(e)}")
            raise
    
    def __del__(self):
        """Clean up database connections."""
        self.source_db.close()
        self.target_db.close()
    
    def _print_table_counts(self, stage: str):
        """Print a table showing row counts for each table in source and target databases."""
        # Skip table counts display in non-verbose mode if it's not the initial display
        if stage == "After Migration" and hasattr(self, 'verbose_mode') and not self.verbose_mode:
            return
            
        logger.info(f"\n=== Table Row Counts ({stage}) ===")
        
        # Get all unique table names
        source_tables = self.get_table_names(self.source_db)
        target_tables = self.get_table_names(self.target_db)
        all_tables = sorted(source_tables.union(target_tables))
        
        # Calculate column widths
        table_width = max(len(table) for table in all_tables) + 2
        count_width = 10
        
        # Print header
        header = f"{'Table':<{table_width}} {'Source':<{count_width}} {'Target':<{count_width}}"
        separator = "-" * (table_width + count_width * 2 + 2)
        logger.info(header)
        logger.info(separator)
        
        # Store counts for report
        self.table_counts[stage.lower()] = {}
        
        # Print row counts for each table
        for table in all_tables:
            source_count = "0"
            target_count = "0"
            
            # Get source count if table exists in source
            if table in source_tables:
                cursor = self.source_db.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                source_count = str(cursor.fetchone()[0])
            
            # Get target count if table exists in target
            if table in target_tables:
                cursor = self.target_db.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                target_count = str(cursor.fetchone()[0])
            
            row = f"{table:<{table_width}} {source_count:<{count_width}} {target_count:<{count_width}}"
            logger.info(row)
            
            # Store counts for report
            self.table_counts[stage.lower()][table] = {
                "source": source_count,
                "target": target_count
            }
        
        # Add prompt after displaying counts
        if stage == "Before Migration":
            # Always prompt for confirmation, regardless of verbose mode
            try:
                input("\nPress [Enter] to continue or [CTRL]+C to abort...")
            except KeyboardInterrupt:
                logger.info("\nMigration aborted by user.")
                raise
    
    def update_person_villages(self):
        """Update village values in hh_person table from household table."""
        logger.info("\nUpdating village values in hh_person table...")
        try:
            # Update hh_person.village using a join with household table
            update_sql = """
                UPDATE hh_person 
                SET village = (
                    SELECT hh.village 
                    FROM household hh 
                    WHERE hh_person.hh_id = hh._id
                )
                WHERE hh_person.hh_id IS NOT NULL
            """
            
            cursor = self.target_db.cursor()
            cursor.execute(update_sql)
            self.target_db.commit()
            
            # Get the number of rows updated
            cursor.execute("SELECT changes()")
            rows_updated = cursor.fetchone()[0]
            
            logger.info(f"Updated village values for {rows_updated} records in hh_person table")
        except Exception as e:
            self.target_db.rollback()
            logger.error(f"Error updating village values: {str(e)}")
            raise

    def migrate_all(self):
        """Migrate all tables from source to target database."""
        # Print initial table counts
        self._print_table_counts("Before Migration")
        
        # Get table names from both databases
        source_tables = self.get_table_names(self.source_db)
        target_tables = self.get_table_names(self.target_db)
        
        # Find tables that exist in both databases
        common_tables = source_tables.intersection(target_tables)
        
        # Log table differences
        source_only = source_tables - target_tables
        target_only = target_tables - source_tables
        
        if source_only:
            logger.info(f"\nTables in source but not in target: {', '.join(source_only)}")
            self.migration_stats["source_only_tables"] = list(source_only)
        if target_only:
            logger.info(f"Tables in target but not in source: {', '.join(target_only)}")
            self.migration_stats["target_only_tables"] = list(target_only)
        
        # Create a single progress bar for all tables
        tables_to_migrate = list(common_tables)
        total_tables = len(tables_to_migrate)
        
        logger.info(f"\nMigrating {total_tables} tables...")
        
        # Count total records to migrate for better progress estimation
        total_records = 0
        table_record_counts = {}
        
        for table_name in tables_to_migrate:
            cursor = self.source_db.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            record_count = cursor.fetchone()[0]
            table_record_counts[table_name] = record_count
            total_records += record_count
        
        logger.info(f"Total records to migrate: {total_records}")
        
        # After displaying info and getting user confirmation, clear the console
        # to make room for the progress bar
        print("\n" * 5)
        
        # Store original logger level to restore it later
        original_level = logger.level
        
        # Always silence loggers during migration to avoid interference with progress bar
        # Create a null handler to silence logging
        class NullHandler(logging.Handler):
            def emit(self, record):
                pass
        
        # Temporarily silence all loggers during migration
        root_logger = logging.getLogger()
        original_root_handlers = root_logger.handlers.copy()
        original_root_level = root_logger.level
        root_logger.handlers = []
        root_logger.addHandler(NullHandler())
        root_logger.setLevel(logging.ERROR)
        
        try:
            # Create overall progress bar
            with tqdm(total=total_records, desc="Total migration progress", ncols=100,
                      bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as overall_bar:
                records_migrated = 0
                
                # Modify migrate_table method to update the overall progress bar
                original_migrate_table = self.migrate_table
                
                def migrate_table_with_overall_progress(table_name):
                    nonlocal records_migrated
                    # Get current record count for this table
                    record_count = table_record_counts.get(table_name, 0)
                    
                    # Update progress bar description
                    overall_bar.set_description(f"Migrating {table_name} ({records_migrated}/{total_records} records)")
                    
                    # Store the original total_records_migrated count
                    original_count = self.migration_stats["total_records_migrated"]
                    
                    # Call the original migrate_table method
                    original_migrate_table(table_name)
                    
                    # Calculate how many records were actually migrated
                    new_count = self.migration_stats["total_records_migrated"]
                    records_migrated_for_table = new_count - original_count
                    
                    # Update the overall progress bar
                    overall_bar.update(records_migrated_for_table)
                    records_migrated += records_migrated_for_table
                
                # Temporarily replace the migrate_table method
                self.migrate_table = migrate_table_with_overall_progress
                
                # Migrate common tables
                for table_name in tables_to_migrate:
                    self.migrate_table(table_name)
                    self.migration_stats["tables_migrated"] += 1
        finally:
            # Restore the original migrate_table method
            self.migrate_table = original_migrate_table
            
            # Restore original logger levels
            root_logger.handlers = original_root_handlers
            root_logger.setLevel(original_root_level)
        
        # Update village values in hh_person table
        self.update_person_villages()
        
        # Log summary statistics based on verbose mode
        if hasattr(self, 'verbose_mode') and not self.verbose_mode:
            # In non-verbose mode, just print a simple summary
            print(f"\nMigration complete. {self.migration_stats['total_records_migrated']} records migrated across {self.migration_stats['tables_migrated']} tables.")
        else:
            # In verbose mode, log detailed summary
            self._log_summary()
            # Print final table counts
            self._print_table_counts("After Migration")
    
    def _log_summary(self):
        """Log migration summary statistics."""
        logger.info("\n=== Migration Summary ===")
        if self.migration_stats["test_mode"]:
            logger.info("Running in TEST MODE")
            if self.migration_stats["max_rows_per_table"]:
                logger.info(f"Row limit per table: {self.migration_stats['max_rows_per_table']}")
        
        logger.info(f"Tables migrated: {self.migration_stats['tables_migrated']}")
        logger.info(f"Tables skipped: {self.migration_stats['tables_skipped']}")
        logger.info(f"Total records migrated: {self.migration_stats['total_records_migrated']}")
        
        if self.migration_stats["source_only_tables"]:
            logger.info(f"\nTables in source but not in target: {', '.join(self.migration_stats['source_only_tables'])}")
        if self.migration_stats["target_only_tables"]:
            logger.info(f"Tables in target but not in source: {', '.join(self.migration_stats['target_only_tables'])}")
        
        if self.migration_stats["source_only_columns"]:
            logger.info("\nColumns in source but not in target:")
            for table, columns in self.migration_stats["source_only_columns"].items():
                logger.info(f"  {table}: {', '.join(columns)}")
        
        if self.migration_stats["target_only_columns"]:
            logger.info("\nColumns in target but not in source:")
            for table, columns in self.migration_stats["target_only_columns"].items():
                logger.info(f"  {table}: {', '.join(columns)}")
        
        if self.migration_stats["pseudotype_conversions"]:
            logger.info("\nPseudotype Conversions Applied:")
            for table, columns in self.migration_stats["pseudotype_conversions"].items():
                logger.info(f"\n  Table: {table}")
                for column, info in columns.items():
                    logger.info(f"    Column: {column} ({info['source_type']} -> {info['target_type']})")
                    for example in info["examples"]:
                        logger.info(f"      Example: {example['original']} -> {example['converted']}")
        
        if self.migration_stats.get("unsupported_conversions"):
            logger.info("\nUnsupported Pseudotype Conversions (No Conversion Applied):")
            for table, columns in self.migration_stats["unsupported_conversions"].items():
                logger.info(f"\n  Table: {table}")
                for column, info in columns.items():
                    logger.info(f"    Column: {column} ({info['source_type']} -> {info['target_type']})")
                    logger.info(f"      Example value: {info['example_value']}")
        
        if self.migration_stats["type_conversion_issues"]:
            logger.info("\nType Conversion Issues:")
            for table, columns in self.migration_stats["type_conversion_issues"].items():
                logger.info(f"\n  Table: {table}")
                for column, issues in columns.items():
                    logger.info(f"    Column: {column}")
                    for issue in issues:
                        logger.info(f"      - {issue}") 