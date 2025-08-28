import pytest
from unittest.mock import MagicMock, patch
from app.storage.duckdb_adapter import DuckDBStorage

def test_init_native_mode(user_model):
    """Tests that the adapter correctly identifies native mode."""
    adapter = DuckDBStorage("duckdb:///:memory:", user_model)
    assert adapter._mode == 'native'

def test_init_attached_mode(user_model):
    """Tests that the adapter correctly identifies attached mode."""
    adapter = DuckDBStorage("postgresql://user:pass@host/db", user_model)
    assert adapter._mode == 'attached'

def test_get_full_table_name(user_model):
    """Tests the table name resolution for both modes."""
    native_adapter = DuckDBStorage("duckdb:///", user_model)
    assert native_adapter._get_full_table_name() == "users"

    attached_adapter = DuckDBStorage("sqlite:///test.db", user_model)
    assert attached_adapter._get_full_table_name() == "attached_db.users"

# Using pytest-mock's `mocker` fixture to patch duckdb.connect
def test_connect_attached_mode_calls(user_model, mocker):
    """
    Tests that the connect method executes the correct SQL commands
    for attached mode without hitting a real database.
    """
    # Patch the duckdb.connect function to return a mock connection
    mock_con = MagicMock()
    mocker.patch('duckdb.connect', return_value=mock_con)

    adapter = DuckDBStorage("postgresql://user:pass@host/db", user_model)
    adapter.connect()

    # Assert that the execute method was called with the expected SQL
    mock_con.execute.assert_any_call("INSTALL postgres;")
    mock_con.execute.assert_any_call("LOAD postgres;")
    mock_con.execute.assert_any_call("ATTACH 'postgresql://user:pass@host/db' AS attached_db (TYPE POSTGRES);")

def test_ensure_table_sql_generation(user_model, mocker):
    """Tests that the correct CREATE TABLE SQL is generated."""
    mock_con = MagicMock()
    mocker.patch('duckdb.connect', return_value=mock_con)

    adapter = DuckDBStorage("duckdb:///:memory:", user_model)
    adapter.connect()
    adapter.ensure_table()

    # Check the generated SQL. The order of columns might vary, so we check parts.
    generated_sql = mock_con.execute.call_args[0][0]
    assert "CREATE TABLE IF NOT EXISTS users" in generated_sql
    assert "id VARCHAR PRIMARY KEY" in generated_sql
    assert "name VARCHAR" in generated_sql
    assert "email VARCHAR" in generated_sql
    assert "is_active BOOLEAN" in generated_sql