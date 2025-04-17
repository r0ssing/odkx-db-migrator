from typing import Dict, List, Optional, Callable, Union, Type, Any
from pydantic import BaseModel
from sqlalchemy import Integer, String, Float, Boolean, DateTime, Date

class DatabaseConfig(BaseModel):
    """Core configuration class for the ODK-X database migration tool.
    
    This class defines the structure for configuring database migrations
    between ODK-X databases with different schemas.
    """
    source_db_path: str
    target_db_path: str
    # Optional custom transformations for specific columns
    column_transformations: Dict[str, Union[Callable, str]] = {}
