from sqlite3 import connect
from typing import List, Dict, Any, Set, Tuple, Callable, Type
from datetime import datetime, date
import logging
from config.schema_config import DatabaseConfig, coerce_to_int, coerce_to_float, coerce_to_bool

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseMigrator:
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.source_db = connect(config.source_db_path)
        self.target_db = connect(config.target_db_path)
        self.migration_stats = {
            "tables_migrated": 0,
            "tables_skipped": 0,
            "total_records_migrated": 0,
            "source_only_tables": [],
            "target_only_tables": [],
            "source_only_columns": {},
            "target_only_columns": {},
            "type_conversion_issues": {},
            "test_mode": config.test_mode,
            "max_rows_per_table": config.max_rows_per_table
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
    
    def migrate_table(self, table_name: str):
        """Migrate a single table, handling column matching and transformations."""
        logger.info(f"\nProcessing table: {table_name}")
        
        # Check if source table has any rows
        cursor = self.source_db.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        source_count = cursor.fetchone()[0]
        if source_count == 0:
            logger.info(f"Skipping table {table_name} - no records in source")
            self.migration_stats["tables_skipped"] += 1
            return
        
        # Get column names from both databases
        source_columns = self.get_column_names(self.source_db, table_name)
        target_columns = self.get_column_names(self.target_db, table_name)
        
        # Find matching and non-matching columns
        matching_columns = source_columns.intersection(target_columns)
        source_only = source_columns - target_columns
        target_only = target_columns - source_columns
        
        # Log column differences
        if source_only:
            logger.info(f"Columns in source but not in target: {', '.join(source_only)}")
            self.migration_stats["source_only_columns"][table_name] = source_only
        if target_only:
            logger.info(f"Columns in target but not in source: {', '.join(target_only)}")
            self.migration_stats["target_only_columns"][table_name] = target_only
        
        if not matching_columns:
            logger.warning(f"No matching columns found for table {table_name}")
            self.migration_stats["tables_skipped"] += 1
            return
        
        try:
            # Fetch records from source with limit if in test mode
            limit_clause = f"LIMIT {self.config.max_rows_per_table}" if self.config.test_mode and self.config.max_rows_per_table else ""
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
                    for i in range(0, len(source_records), batch_size):
                        batch = source_records[i:i + batch_size]
                        # Create placeholders for the batch
                        placeholders = ", ".join(["(" + ", ".join(["?" for _ in matching_columns]) + ")" for _ in batch])
                        
                        # Construct and execute the insert statement
                        insert_sql = f"INSERT INTO {table_name} ({', '.join(matching_columns)}) VALUES {placeholders}"
                        
                        # Flatten the batch for SQLite
                        values = []
                        for record in batch:
                            record_dict = dict(zip(column_names, record))
                            values.extend(record_dict[col] for col in matching_columns)
                        
                        # Execute the insert
                        target_cursor = self.target_db.cursor()
                        target_cursor.execute(insert_sql, values)
                        self.target_db.commit()
                        
                        logger.info(f"Inserted batch of {len(batch)} records")
                    
                    self.migration_stats["total_records_migrated"] += len(source_records)
                    logger.info(f"Successfully migrated {len(source_records)} records")
                    
                    # Verify the insert
                    target_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    actual_count = target_cursor.fetchone()[0]
                    logger.info(f"Verified target table count: {actual_count}")
                    
                except Exception as e:
                    self.target_db.rollback()
                    logger.error(f"Error during insert for table {table_name}: {str(e)}")
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
        
        # Migrate common tables
        for table_name in common_tables:
            self.migrate_table(table_name)
            self.migration_stats["tables_migrated"] += 1
        
        # Update village values in hh_person table
        self.update_person_villages()
        
        # Log summary statistics
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
        
        if self.migration_stats["type_conversion_issues"]:
            logger.info("\nType Conversion Issues:")
            for table, columns in self.migration_stats["type_conversion_issues"].items():
                logger.info(f"\n  Table: {table}")
                for column, issues in columns.items():
                    logger.info(f"    Column: {column}")
                    for issue in issues:
                        logger.info(f"      - {issue}") 