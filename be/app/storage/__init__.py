import os
from typing import Optional, Type

from app.storage.interface import StorageInterface, T
from app.storage.sqlalchemy_adapter import SQLAlchemyStorage
from app.storage.noop_adapter import NoOpStorage
from app.utils import get_bool_env

def get_storage(
    model: Type[T],
    db_url: Optional[str] = None
) -> StorageInterface[T]:
    """
    Factory to select a model-aware storage backend.
    """
    # Check the feature flag first
    if not get_bool_env('LAKEVISION_HEALTH_ENABLED'):
        return NoOpStorage(model) # Return the dummy object

    # If health is enabled, THEN proceed to get the DB URL
    db_url = db_url or os.getenv('LAKEVISION_DATABASE_URL')
    
    if not db_url:
        # This error is now correct: health is ON but DB_URL is missing
        raise ValueError("Health feature is enabled, but LAKEVISION_DATABASE_URL is not provided or set.")

    return SQLAlchemyStorage(db_url, model)