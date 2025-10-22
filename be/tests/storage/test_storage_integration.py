import pytest
from datetime import datetime, timezone

# Note: The dataclass definitions and all fixtures are now in conftest.py
# We don't need to define them here.

# --- Tests for Basic User Model ----------------------------------------
# These tests will use the `storage_adapter` fixture, which is pre-configured
# for the User model by conftest.py.

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
    
    storage.save_many([user1, user2])
    
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
    
    storage.save_many([user1, user2, user3])
    
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

def test_save_many_upsert_logic(storage_adapter, user_model):
    """Test that save_many correctly adds new and updates existing items."""
    storage = storage_adapter
    user_v1 = user_model(id="sm:1", name="User 1 Original", email="1@a.com")
    user_v2 = user_model(id="sm:2", name="User 2 Original", email="2@b.com")
    storage.save_many([user_v1, user_v2])
    
    # Check initial state
    assert storage.get_by_id("sm:1").name == "User 1 Original"
    assert storage.get_by_id("sm:2").name == "User 2 Original"
    
    # Prepare an upsert batch
    user_v1_updated = user_model(id="sm:1", name="User 1 Updated", email="1@a.com")
    user_v3_new = user_model(id="sm:3", name="User 3 New", email="3@c.com")
    
    storage.save_many([user_v1_updated, user_v3_new])
    
    # FIX: Replace get_all_count() with get_aggregate("COUNT", "*")
    assert storage.get_aggregate("COUNT", "*") == 3 # Make sure user:2 wasn't deleted
    assert storage.get_by_id("sm:1").name == "User 1 Updated"
    assert storage.get_by_id("sm:2").name == "User 2 Original"
    assert storage.get_by_id("sm:3").name == "User 3 New"

# --- Tests for Advanced Features using ComplexItem Model -----------------
# These tests will use the `complex_item_storage` fixture, which is pre-configured
# for the ComplexItem model by conftest.py.

def test_save_and_get_complex_types(complex_item_storage, complex_item_model):
    """Test saving and retrieving fields serialized as JSON (list, dict)."""
    storage = complex_item_storage
    item = complex_item_model(
        id="item:1",
        name="Complex",
        age=10,
        tags=["a", "b", "c"],
        metadata={"owner": "admin", "nested": {"key": 1}}
    )
    storage.save(item)
    
    retrieved = storage.get_by_id("item:1")
    
    assert retrieved is not None
    assert retrieved.tags == ["a", "b", "c"]
    assert retrieved.metadata == {"owner": "admin", "nested": {"key": 1}}
    assert retrieved == item

def test_save_and_get_datetime(complex_item_storage, complex_item_model):
    """Test saving and retrieving native datetime objects."""
    storage = complex_item_storage
    now = datetime.now(timezone.utc)
    item = complex_item_model(id="item:dt", name="Datetime Test", age=1, created_at=now)
    
    storage.save(item)
    retrieved = storage.get_by_id("item:dt")
    
    assert retrieved is not None
    assert isinstance(retrieved.created_at, datetime)
    # Compare timestamps to account for potential microsecond rounding by DB
    assert retrieved.created_at.timestamp() == pytest.approx(now.timestamp())

def test_get_by_attributes_multiple_criteria(complex_item_storage, complex_item_model):
    """Test querying with multiple 'AND' criteria."""
    storage = complex_item_storage
    item1 = complex_item_model(id="multi:1", name="Test", age=20, status="active")
    item2 = complex_item_model(id="multi:2", name="Test", age=30, status="active")
    item3 = complex_item_model(id="multi:3", name="Test", age=20, status="pending")
    storage.save_many([item1, item2, item3])
    
    results = storage.get_by_attributes({"name": "Test", "age": 20})
    assert len(results) == 2
    assert {item1.id, item3.id} == {i.id for i in results}
    
    results_2 = storage.get_by_attributes({"name": "Test", "status": "active"})
    assert len(results_2) == 2
    assert {item1.id, item2.id} == {i.id for i in results_2}
    
    results_3 = storage.get_by_attributes({"name": "Test", "status": "active", "age": 20})
    assert len(results_3) == 1
    assert item1 in results_3

def test_get_by_attributes_in_clause(complex_item_storage, complex_item_model):
    """Test querying using an 'IN' clause (list of values)."""
    storage = complex_item_storage
    item1 = complex_item_model(id="in:1", name="Apple", age=1)
    item2 = complex_item_model(id="in:2", name="Banana", age=1)
    item3 = complex_item_model(id="in:3", name="Cherry", age=1)
    storage.save_many([item1, item2, item3])
    
    results = storage.get_by_attributes({"name": ["Apple", "Cherry"]})
    assert len(results) == 2
    assert {item1.id, item3.id} == {i.id for i in results}

def test_get_by_attributes_in_clause_empty(complex_item_storage, complex_item_model):
    """Test that an empty 'IN' list correctly returns no results."""
    storage = complex_item_storage
    item1 = complex_item_model(id="in:empty", name="Test", age=1)
    storage.save(item1)
    
    results = storage.get_by_attributes({"name": []})
    assert len(results) == 0

def test_get_by_attributes_is_null(complex_item_storage, complex_item_model):
    """Test querying for 'IS NULL' using None."""
    storage = complex_item_storage
    item1 = complex_item_model(id="null:1", name="Has Status", age=1, status="active")
    item2 = complex_item_model(id="null:2", name="No Status", age=1, status=None)
    storage.save_many([item1, item2])
    
    results = storage.get_by_attributes({"status": None})
    assert len(results) == 1
    assert item2 in results

def test_get_by_attributes_pagination(complex_item_storage, complex_item_model):
    """Test 'skip' and 'limit' pagination."""
    storage = complex_item_storage
    now = datetime.now(timezone.utc)
    items = []
    for i in range(5):
        # We must set created_at for deterministic ordering
        items.append(complex_item_model(id=f"page:{i}", name="Page", age=i, created_at=now.replace(microsecond=i*1000)))
    storage.save_many(items)
    
    # Get all 5, ordered by created_at DESC (item 4, 3, 2, 1, 0)
    all_items = storage.get_by_attributes({"name": "Page"})
    assert [i.age for i in all_items] == [4, 3, 2, 1, 0]
    
    # Get limit=2
    page1 = storage.get_by_attributes({"name": "Page"}, limit=2)
    assert len(page1) == 2
    assert [i.age for i in page1] == [4, 3]
    
    # Get limit=2, skip=2
    page2 = storage.get_by_attributes({"name": "Page"}, skip=2, limit=2)
    assert len(page2) == 2
    assert [i.age for i in page2] == [2, 1]

def test_delete_by_attributes(complex_item_storage, complex_item_model):
    """Test deleting records matching specific criteria."""
    storage = complex_item_storage
    item1 = complex_item_model(id="del:1", name="DeleteMe", age=10, status="pending")
    item2 = complex_item_model(id="del:2", name="DeleteMe", age=20, status="active")
    item3 = complex_item_model(id="del:3", name="KeepMe", age=10, status="pending")
    storage.save_many([item1, item2, item3])
    
    # Delete where name="DeleteMe" AND status="pending"
    deleted_count = storage.delete_by_attributes({"name": "DeleteMe", "status": "pending"})
    
    assert deleted_count == 1
    assert storage.get_by_id("del:1") is None
    assert storage.get_by_id("del:2") is not None
    assert storage.get_by_id("del:3") is not None

def test_delete_by_attributes_in_clause(complex_item_storage, complex_item_model):
    """Test deleting records using an 'IN' clause."""
    storage = complex_item_storage
    item1 = complex_item_model(id="del_in:1", name="A", age=1)
    item2 = complex_item_model(id="del_in:2", name="B", age=1)
    item3 = complex_item_model(id="del_in:3", name="C", age=1)
    storage.save_many([item1, item2, item3])
    
    # Delete where name IN ["A", "C"]
    deleted_count = storage.delete_by_attributes({"name": ["A", "C"]})
    
    assert deleted_count == 2
    assert storage.get_by_id("del_in:1") is None
    assert storage.get_by_id("del_in:2") is not None
    assert storage.get_by_id("del_in:3") is None

def test_delete_by_attributes_safety_check(complex_item_storage):
    """Test that deleting with empty criteria raises a ValueError."""
    storage = complex_item_storage
    with pytest.raises(ValueError, match="requires at least one criterion"):
        storage.delete_by_attributes({})

def test_get_aggregate(complex_item_storage, complex_item_model):
    """Test aggregation functions (COUNT, SUM, AVG, MIN, MAX)."""
    storage = complex_item_storage
    storage.save_many([
        complex_item_model(id="agg:1", name="A", age=10, status="active"),
        complex_item_model(id="agg:2", name="B", age=20, status="active"),
        complex_item_model(id="agg:3", name="C", age=30, status="pending"),
    ])
    
    assert storage.get_aggregate("COUNT", "*") == 3
    assert storage.get_aggregate("SUM", "age") == 60
    assert storage.get_aggregate("AVG", "age") == 20
    assert storage.get_aggregate("MIN", "age") == 10
    assert storage.get_aggregate("MAX", "age") == 30
    
    # Test with criteria
    count_active = storage.get_aggregate("COUNT", "*", criteria={"status": "active"})
    assert count_active == 2
    
    sum_active = storage.get_aggregate("SUM", "age", criteria={"status": "active"})
    assert sum_active == 30

def test_get_aggregate_group_by(complex_item_storage, complex_item_model):
    """Test aggregation with a 'GROUP BY' clause."""
    storage = complex_item_storage
    storage.save_many([
        complex_item_model(id="group:1", name="A", age=10, status="active"),
        complex_item_model(id="group:2", name="B", age=20, status="active"),
        complex_item_model(id="group:3", name="C", age=30, status="pending"),
        complex_item_model(id="group:4", name="D", age=40, status="active"),
    ])
    
    # Get average age grouped by status
    results = storage.get_aggregate("AVG", "age", group_by=["status"])
    
    assert isinstance(results, list)
    assert len(results) == 2
    # Convert to dict for easier assertion
    results_dict = {r["status"]: r["result"] for r in results}
    assert results_dict["active"] == pytest.approx((10 + 20 + 40) / 3) # 23.33...
    assert results_dict["pending"] == 30

def test_get_aggregate_invalid_function_raises(complex_item_storage):
    """Test that an unsupported function raises a ValueError."""
    storage = complex_item_storage
    with pytest.raises(ValueError, match="Unsupported aggregate function: 'MEDIAN'"):
        storage.get_aggregate("MEDIAN", "age")

def test_find_by_raw_query_deserializes(complex_item_storage, complex_item_model):
    """Test that find_by_raw_query returns deserialized model instances."""
    storage = complex_item_storage
    now = datetime.now(timezone.utc)
    item = complex_item_model(
        id="raw:1",
        name="Raw Test",
        age=5,
        tags=["raw", "sql"],
        created_at=now
    )
    storage.save(item)
    
    # Use raw SQL with a parameter
    query = f"SELECT * FROM {storage.table_name} WHERE name = :name"
    params = {"name": "Raw Test"}
    
    results = storage.find_by_raw_query(query, params)
    
    assert len(results) == 1
    retrieved = results[0]
    
    assert isinstance(retrieved, complex_item_model)
    assert retrieved.id == "raw:1"
    assert retrieved.tags == ["raw", "sql"]
    assert isinstance(retrieved.created_at, datetime)

def test_execute_raw_select_query_non_select_raises(storage_adapter):
    """Test that execute_raw_select_query blocks non-SELECT statements."""
    storage = storage_adapter
    
    table_name = storage.table_name
    with pytest.raises(ValueError, match="only supports SELECT queries"):
        storage.execute_raw_select_query(f"DELETE FROM {table_name} WHERE 1=1")
        
    with pytest.raises(ValueError, match="only supports SELECT queries"):
        storage.execute_raw_select_query(f"  UPDATE {table_name} SET name = 'hacked'")
