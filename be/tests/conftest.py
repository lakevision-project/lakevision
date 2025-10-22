import pytest
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import json
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock

from starlette.middleware.sessions import SessionMiddleware

# Set env var for in-memory DB for all tests
os.environ['DATABASE_URL'] = 'sqlite:///:memory:'
os.environ['PYICEBERG_CATALOG__DEFAULT__URI'] = 'sqlite:///:memory:'
os.environ['AUTH_ENABLED'] = 'False' # Disable auth globally for tests unless overridden
os.environ['SECRET_KEY'] = 'test-secret-key-for-sessions' # Needed for session middleware

@pytest.fixture(scope='session', autouse=True)
def mock_global_dependencies():
    """
    Globally mocks dependencies that are initialized on app startup
    to prevent real database/network calls during test collection and setup.
    This runs once for the entire test session, before any other fixtures.
    """
    # We patch the dependencies where they are defined to ensure they are
    # replaced before the FastAPI app loads them.
    with patch('app.dependencies.lv', MagicMock()) as mock_lv, \
         patch('app.dependencies.authz_', MagicMock()) as mock_authz:
        yield mock_lv, mock_authz

from app.api import app
from app.storage import get_storage
# We need to import the class directly to patch it for testing
from app.storage import SQLAlchemyStorage
from contextlib import contextmanager

app.add_middleware(SessionMiddleware, secret_key=os.environ['SECRET_KEY'])

@pytest.fixture(scope="module")
def client():
    """
    Provides a FastAPI TestClient that is configured once for the entire module.
    """
    with TestClient(app) as c:
        yield c

@pytest.fixture(autouse=True)
def override_dependencies():
    """
    Fixture to automatically clear dependency overrides after each test.
    This ensures tests are isolated from each other.
    """
    original_overrides = app.dependency_overrides.copy()
    yield
    # This code runs after each test to clean up
    app.dependency_overrides = original_overrides


# --- Model Definitions for Tests ---

@dataclass
class User:
    """A simple dataclass representing a user, for testing purposes."""
    id: str
    name: str
    email: str
    is_active: bool = True

@dataclass
class ComplexItem:
    """A model with all the complex types we need to test."""
    id: str
    name: str
    age: int
    status: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: Optional[datetime] = None

# --- Model Fixtures ---

@pytest.fixture
def user_model():
    """Provides the User dataclass model to tests."""
    return User

@pytest.fixture
def complex_item_model():
    """Provides the ComplexItem dataclass model to tests."""
    return ComplexItem

# --- Storage Fixtures ---

@pytest.fixture(scope="function")
def storage_adapter_factory(monkeypatch):
    """
    A factory fixture that creates and sets up a storage adapter for a given model.
    Handles connection/disconnection and patches datetime deserialization for SQLite tests.
    """
    # --- FIX: Patch the deserialization method to handle SQLite's string datetimes ---
    original_deserialize = SQLAlchemyStorage._deserialize_row

    def patched_deserialize_row(self, row_dict: Dict[str, Any]) -> Dict[str, Any]:
        # First, run the original method to handle JSON fields
        deserialized = original_deserialize(self, row_dict)
        
        # Now, add the missing logic for datetime fields
        for field_name in self._datetime_fields:
            if field_name in deserialized and isinstance(deserialized[field_name], str):
                try:
                    # SQLite often stores datetimes like '2025-10-19 19:36:00.123456'
                    # fromisoformat can handle this. We assume UTC if no timezone is present.
                    dt_obj = datetime.fromisoformat(deserialized[field_name])
                    if dt_obj.tzinfo is None:
                        dt_obj = dt_obj.replace(tzinfo=timezone.utc)
                    deserialized[field_name] = dt_obj
                except (ValueError, TypeError):
                    # If parsing fails for any reason, leave the original string value
                    pass
        return deserialized

    monkeypatch.setattr(SQLAlchemyStorage, '_deserialize_row', patched_deserialize_row)
    
    created_adapters = []
    def _factory(model_class):
        adapter = get_storage(model=model_class, db_url="sqlite:///:memory:")
        adapter.connect()
        adapter.ensure_table()
        created_adapters.append(adapter)
        return adapter

    yield _factory

    # Teardown: disconnect all adapters created by the factory
    for adapter in created_adapters:
        adapter.disconnect()

@pytest.fixture
def storage_adapter(storage_adapter_factory, user_model):
    """Provides a ready-to-use storage adapter for the simple User model."""
    return storage_adapter_factory(user_model)

@pytest.fixture
def complex_item_storage(storage_adapter_factory, complex_item_model):
    """Provides a ready-to-use storage adapter for the ComplexItem model."""
    return storage_adapter_factory(complex_item_model)

