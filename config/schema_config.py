from typing import Dict, List, Optional, Callable, Union, Type, Any
from pydantic import BaseModel
from sqlalchemy import text, Integer, String, Float, Boolean, DateTime, Date
import logging

logger = logging.getLogger(__name__)

class DatabaseConfig(BaseModel):
    source_db_path: str
    target_db_path: str
    # Optional custom transformations for specific columns
    column_transformations: Dict[str, Union[Callable, str]] = {}
    # Testing options
    test_mode: bool = False
    max_rows_per_table: Optional[int] = None

def coerce_to_int(value: Any) -> Optional[int]:
    """Coerce value to integer, handling various input formats."""
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            # Remove any non-numeric characters except minus sign
            cleaned = ''.join(c for c in value if c.isdigit() or c == '-')
            return int(cleaned) if cleaned else None
        return None
    except (ValueError, TypeError):
        logger.warning(f"Failed to coerce value '{value}' to integer")
        return None

def coerce_to_float(value: Any) -> Optional[float]:
    """Coerce value to float, handling various input formats."""
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            # Remove any non-numeric characters except decimal point and minus sign
            cleaned = ''.join(c for c in value if c.isdigit() or c in '.-')
            return float(cleaned) if cleaned else None
        return None
    except (ValueError, TypeError):
        logger.warning(f"Failed to coerce value '{value}' to float")
        return None

def coerce_to_bool(value: Any) -> Optional[bool]:
    """Coerce value to boolean, handling various input formats."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        value = value.lower().strip()
        if value in ('true', '1', 'yes', 'on'):
            return True
        if value in ('false', '0', 'no', 'off'):
            return False
    if isinstance(value, (int, float)):
        return bool(value)
    return None

def get_village_from_household(engine, record):
    """Get village value from household table using a join."""
    query = text("""
        SELECT hh.village 
        FROM hh_person person 
        LEFT JOIN household hh ON person.hh_id = hh._id 
        WHERE person._id = :person_id
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"person_id": record._id}).first()
        return result[0] if result else None

# Example schema configuration
SCHEMA_CONFIG = DatabaseConfig(
    source_db_path="data/source.db",
    target_db_path="data/target.db",
    column_transformations={
        "name": lambda x: x.title() if x else None,  # Capitalize names
        "village": get_village_from_household,  # Complex transformation using SQL join
    },
    # Testing options
    test_mode=True,  # Enable test mode
    max_rows_per_table=None  # No limit on rows per table
)