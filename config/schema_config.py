from typing import Dict, Optional
from sqlalchemy import text
import logging

from src.models import DatabaseConfig

logger = logging.getLogger(__name__)

# Project-specific transformation functions
def get_village_from_household(engine, record):
    """Get village value from household table using a join.
    
    This is a project-specific transformation function that demonstrates
    how to perform complex transformations using SQL joins.
    """
    query = text("""
        SELECT hh.village 
        FROM hh_person person 
        LEFT JOIN household hh ON person.hh_id = hh._id 
        WHERE person._id = :person_id
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"person_id": record._id}).first()
        return result[0] if result else None

# Project-specific schema configuration
SCHEMA_CONFIG = DatabaseConfig(
    source_db_path="data/source.db",
    target_db_path="data/target.db",
    column_transformations={
        "name": lambda x: x.title() if x else None,  # Capitalize names
        "village": get_village_from_household,  # Complex transformation using SQL join
    }
)