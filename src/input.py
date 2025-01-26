import json
from datetime import datetime, timedelta
import polars as pl
from pydantic import BaseModel

##TODO: If many more validation rules are added, consider to make subdirectory for validation per field

def validate_timestamp(timestamp):
    """Validate timestamp format and check constraints.
    Constraints are that the timestamp should not be in the future and should not be more than 1 week old.
    """
    try:
        dt = datetime.fromisoformat(timestamp.replace('Z', ''))
        now = datetime.utcnow()
        if dt > now:
            raise ValueError("Timestamp cannot be in the future")
        if dt < now - timedelta(weeks=1):
            raise ValueError("Timestamp cannot be more than 1 week old")
    except ValueError:
        raise ValueError("Invalid timestamp format")

def validate_record(record):
    """
    Validate individual record payload for required fields and format.
    """
    # TODO: add other validations as needed
    validate_timestamp(timestamp=record['timestamp'])



