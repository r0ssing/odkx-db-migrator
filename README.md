# ODK-X Database Migration Tool

This tool helps migrate data from one ODK-X SQLite database to another with a different schema. It supports:
- Table-by-table migration
- Column mapping between source and target tables
- Custom transformation logic for new columns
- Schema validation
- Pseudotype conversions (e.g., string to array)
- Attachment management and resizing

## Quick Start

1. **View available commands:**
```bash
python helpers.py
```

2. **Set up the source and target databases:**
```bash
# Pull the database from the current ODK-X app (version N) and save as source.db
python helpers.py pull_database --target_file source.db

# Pull the database from the new ODK-X app (version N+1) and save as target.db
python helpers.py pull_database
```

3. **Run the migration:**
```bash
python helpers.py migrate [--table TABLE_NAME] [--verbose]
```

4. **Manage attachments:**
```bash
python resize.py
```

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
│   └── attachments/       # Directory for attachment files (same structure as on device)
|       └── <table_name>/instances/<instance_id>/<attachment_name>.jpg
|       └── <table_name>/instances/<instance_id>/<attachment_name>.jpg
|       └── ...
├── helpers.py            # Helper functions for database and attachment management
├── resize.py             # Attachment resizing and analysis tools
└── requirements.txt
```

## Database Setup

### Source Database
The source database (`data/source.db`) should be a populated database from the current version of the ODK-X app:

```bash
# Connect your Android device with the current ODK-X app installed
python helpers.py pull_database --target_file source.db
```

### Target Database
The target database (`data/target.db`) should be an empty database created by the new version of the ODK-X app:

```bash
# Install the new version of the ODK-X app on your device
# Run the initialization logic (i.e., start Tables/Survey apps)
python helpers.py pull_database
```

## Setup

1. **Create a virtual environment:**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. **Install dependencies:**
```bash
pip install -r requirements.txt
```

3. **Configure your schema** in `config/schema_config.py`

4. **Run the migration:**
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

### ODK-X Sync Utilities (sync.py)
Run `python sync.py` to see all available functions for interacting with the ODK-X sync endpoint. This tool is designed for making minor updates to the app without requiring a full reset of the sync endpoint, such as uploading new CSS or JavaScript files without changes to the data structure.

Available commands include:
- `setCredentials` - Set server credentials for synchronization
- `checkAuth` - Verify if the user has sufficient permissions to download data
- `pushFile` - Upload one or more files to the ODK server
- `deleteFile` - Delete one or more files from the ODK server

Example usage:
```bash
# Set credentials for the ODK-X server
python sync.py setCredentials --server "https://your-server.org" --username "user" --password "pass"

# Check if authentication is successful
python sync.py checkAuth

# Upload multiple files to a specific folder
python sync.py pushFile --path "path/to/file1.css, path/to/file2.js" --remoteFolder "assets/dist/"

# Delete multiple files
python sync.py deleteFile --path "assets/dist/old-file1.css, assets/dist/old-file2.js"
```

## Configuration
Edit `config/schema_config.py` to define:
- Source and target table schemas
- Column mappings
- Custom transformation logic for new columns

Pseudotype conversions (e.g., string to array) are automatically handled based on the _column_definitions table in the ODK-X databases, so no explicit configuration is needed for this feature.
