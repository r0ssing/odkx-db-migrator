from src.migrator import DatabaseMigrator
from config.schema_config import SCHEMA_CONFIG
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    try:
        # Initialize migrator
        migrator = DatabaseMigrator(SCHEMA_CONFIG)
        
        # Run migration
        migrator.migrate_all()
        
    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        raise

if __name__ == "__main__":
    main() 