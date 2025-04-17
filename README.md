# ODK-X Database Migration Tool

This tool helps migrate data from one ODK-X SQLite database to another with a different schema. It supports:
- Table-by-table migration
- Column mapping between source and target tables
- Custom transformation logic for new columns
- Schema validation
- Pseudotype conversions (e.g., string to array)
- Attachment management and resizing

## Project Structure
```
datamigration/
├── config/
│   └── schema_config.py    # Schema definitions and column mappings
├── src/
│   ├── __init__.py
│   ├── migrator.py        # Main migration logic
│   ├── transformers.py    # Custom transformation functions
│   └── utils.py           # Utility functions
├── data/
│   ├── source.db          # Source database (from current ODK-X app (ie version *N*))
│   ├── target.db          # Target database (from new ODK-X app (ie version *N+1*))
│   └── attachments/       # Directory for attachment files (to be resized, pruned, pushed to device, etc.)
├── helpers.py            # Helper functions for database and attachment management
├── resize.py             # Attachment resizing and analysis tools
└── requirements.txt
```

## Database Setup

### Source Database
The source database (`data/source.db`) should be a populated database from the current version of the ODK-X app:
1. Connect your Android device with the current ODK-X app installed
2. Use the `pull_database` helper function to pull the database from the device:
   ```bash
   python helpers.py pull_database
   ```
3. Rename the pulled database to `source.db`:
   ```bash
   mv data/target.db data/source.db
   ```

### Target Database
The target database (`data/target.db`) should be an empty database created by the new version of the ODK-X app:
1. Install the new version of the ODK-X app on your device
2. Run the initialization logic (i.e., start Tables/Survey apps)
3. Use the `pull_database` helper function to pull the initialized database:
   ```bash
   python helpers.py pull_database
   ```

## Setup
1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure your schema in `config/schema_config.py`

4. Run the migration:
```bash
python helpers.py migrate [--table TABLE_NAME] [--verbose]
```

## Helper Functions
The project includes two utility scripts with helpful functions:

### Database and Attachment Management (helpers.py)
Run `python helpers.py` to see all available helper functions for database management, including:
- Pull/push databases to/from Android devices
- Clean up databases
- Manage form tables and attachments

### Attachment Resizing and Analysis (resize.py)
Run `python resize.py` to see all available functions for attachment management, including:
- Get attachment size statistics
- Resize image attachments to reduce file size

## Configuration
Edit `config/schema_config.py` to define:
- Source and target table schemas
- Column mappings
- Custom transformation logic for new columns
- Pseudotype handling for ODK-X specific data types
