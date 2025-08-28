import pytest

# The `storage_adapter` fixture will run each test below twice:
# 1. With the `duckdb_in_memory_storage` fixture.
# 2. With the `sqlalchemy_in_memory_storage` fixture.

def test_save_and_get_by_id(storage_adapter, user_model):
    """Test saving a new item and retrieving it."""
    storage = storage_adapter
    user = user_model(id="user:1", name="Alice", email="alice@example.com")
    
    storage.save(user)
    retrieved_user = storage.get_by_id("user:1")
    
    assert retrieved_user is not None
    assert retrieved_user == user

def test_get_by_id_not_found(storage_adapter):
    """Test that getting a non-existent ID returns None."""
    storage = storage_adapter
    retrieved_user = storage.get_by_id("user:nonexistent")
    assert retrieved_user is None

def test_save_is_upsert(storage_adapter, user_model):
    """Test that saving an existing item updates it."""
    storage = storage_adapter
    user = user_model(id="user:2", name="Bob", email="bob@example.com")
    storage.save(user)
    
    # Modify and save again
    user.name = "Robert"
    storage.save(user)
    
    retrieved_user = storage.get_by_id("user:2")
    assert retrieved_user.name == "Robert"

def test_get_all(storage_adapter, user_model):
    """Test retrieving all items from the table."""
    storage = storage_adapter
    user1 = user_model(id="user:3", name="Charlie", email="charlie@example.com")
    user2 = user_model(id="user:4", name="Dana", email="dana@example.com")
    
    storage.save(user1)
    storage.save(user2)
    
    all_users = storage.get_all()
    assert len(all_users) == 2
    # Convert to sets to ignore order
    assert {user1.id, user2.id} == {u.id for u in all_users}

def test_delete(storage_adapter, user_model):
    """Test deleting an item."""
    storage = storage_adapter
    user = user_model(id="user:5", name="Eve", email="eve@example.com")
    storage.save(user)
    
    # Verify it's there
    assert storage.get_by_id("user:5") is not None
    
    # Delete and verify it's gone
    storage.delete("user:5")
    assert storage.get_by_id("user:5") is None

def test_get_by_attribute(storage_adapter, user_model):
    """Test querying by a specific attribute."""
    storage = storage_adapter
    user1 = user_model(id="user:6", name="Frank", email="frank@a.com", is_active=True)
    user2 = user_model(id="user:7", name="Grace", email="grace@b.com", is_active=False)
    user3 = user_model(id="user:8", name="Frank", email="frank@c.com", is_active=True)
    
    storage.save(user1)
    storage.save(user2)
    storage.save(user3)
    
    # Find all users named "Frank"
    franks = storage.get_by_attribute("name", "Frank")
    assert len(franks) == 2
    assert {user1.id, user3.id} == {u.id for u in franks}
    
    # Find all inactive users
    inactive_users = storage.get_by_attribute("is_active", False)
    assert len(inactive_users) == 1
    assert user2 in inactive_users

def test_get_by_attribute_not_found(storage_adapter):
    """Test querying by attribute with no matches."""
    storage = storage_adapter
    results = storage.get_by_attribute("name", "Heidi")
    assert results == []

def test_get_by_invalid_attribute_raises_error(storage_adapter):
    """Test that querying by a non-existent attribute raises a ValueError."""
    storage = storage_adapter
    with pytest.raises(ValueError, match="'invalid_field' is not a valid field in User"):
        storage.get_by_attribute("invalid_field", "some_value")