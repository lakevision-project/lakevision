import os
from typing import Optional, Type

from app.storage.interface import StorageInterface, T
from app.storage.sqlalchemy_adapter import SQLAlchemyStorage
from app.insights.rules import InsightRun

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

run_storage = get_storage(model=InsightRun)
run_storage.connect()
run_storage.ensure_table()
run_storage.disconnect()