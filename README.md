# SQLite Data Migration Tool

This tool helps migrate data from one SQLite database to another with a different schema. It supports:
- Table-by-table migration
- Column mapping between source and target tables
- Custom transformation logic for new columns
- Schema validation

## Project Structure
```
datamigration/
├── config/
│   └── schema_config.py    # Schema definitions and column mappings
├── src/
│   ├── __init__.py
│   ├── migrator.py        # Main migration logic
│   └── transformers.py    # Custom transformation functions
├── data/
│   ├── source.db          # Source database
│   └── target.db          # Target database
├── requirements.txt
└── main.py               # Entry point
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
python main.py
```

## Configuration
Edit `config/schema_config.py` to define:
- Source and target table schemas
- Column mappings
- Custom transformation logic for new columns # odkx-db-migrator
