from typing import Any
from datetime import datetime

def extract_date(datetime_str: str) -> str:
    """Extract date from datetime string."""
    try:
        dt = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return datetime_str

def capitalize_string(value: str) -> str:
    """Capitalize the first letter of each word in a string."""
    if not isinstance(value, str):
        return value
    return value.title()

def combine_fields(field1: Any, field2: Any) -> str:
    """Combine two fields with a space between them."""
    return f"{field1} {field2}".strip()

# Add more transformation functions as needed 