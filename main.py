from helpers import migrate
import sys
import argparse

def main():
    # Parse arguments
    parser = argparse.ArgumentParser(description="Run database migration")
    parser.add_argument("--table", type=str, help="Migrate a specific table")
    args = parser.parse_args()
    
    # Run migration with optional table parameter
    migrate(table_name=args.table)

if __name__ == "__main__":
    main()