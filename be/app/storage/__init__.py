import os
from typing import Optional, Type

from .interface import StorageInterface, T
from .sqlalchemy_adapter import SQLAlchemyStorage

def get_storage(
    model: Type[T],
    db_url: Optional[str] = None
) -> StorageInterface[T]:
    """
    Factory to select a model-aware storage backend.
    """
    db_url = db_url or os.getenv('DATABASE_URL')
    if not db_url:
        raise ValueError("Database URL is not provided or set in DATABASE_URL.")

    return SQLAlchemyStorage(db_url, model)