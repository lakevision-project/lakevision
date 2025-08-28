import os
from typing import Optional, Type

from .interface import StorageInterface, T
from .sqlalchemy_adapter import SQLAlchemyStorage
from .duckdb_adapter import DuckDBStorage

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

    scheme = db_url.split("://")[0]
    if scheme == 'duckdb':
        return DuckDBStorage(db_url, model)
    elif scheme in ['postgresql', 'sqlite', 'mysql']:
        return SQLAlchemyStorage(db_url, model)
    else:
        raise ValueError(f"Unsupported database scheme for storage: {scheme}")