import pytest
import os
from dataclasses import dataclass
from app.storage import get_storage
from app.storage.interface import StorageInterface

@dataclass
class User:
    """A simple dataclass representing a user, for testing purposes."""
    id: str
    name: str
    email: str
    is_active: bool = True

@pytest.fixture
def user_model():
    """Provides the User dataclass model to tests."""
    return User


@pytest.fixture
def sqlalchemy_in_memory_storage(user_model):
    """Provides a connected, in-memory SQLAlchemy (SQLite) storage adapter."""
    storage = get_storage(model=user_model, db_url="sqlite:///:memory:")
    try:
        storage.connect()
        storage.ensure_table()
        yield storage
    finally:
        storage.disconnect()

@pytest.fixture(params=["sqlalchemy_in_memory_storage"])
def storage_adapter(request):
    """
    A meta-fixture that parameterizes tests to run against all storage adapters.
    `request.getfixturevalue` is a pytest feature to call a fixture by its name string.
    """
    return request.getfixturevalue(request.param)