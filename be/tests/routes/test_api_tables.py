import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
import pandas as pd

from app.api import app
from app.dependencies import get_table, check_auth

# The 'client' fixture is provided by conftest.py

# --- Namespace and Table Listing Tests ---

def test_read_namespaces(client: TestClient):
    """Test retrieving a list of namespaces, forcing a refresh to bypass caching."""
    # Patch the 'lv' object where it is looked up (in the API module), not where it is defined.
    # Also patch the global list it modifies to ensure test isolation.
    with patch('app.api.tables.lv') as mock_lv, \
         patch('app.dependencies.namespaces', []) as mock_namespaces_list:
        
        # Configure the mock lv to return a sample list of namespaces
        mock_lv.get_namespaces.return_value = [("ns1",), ("ns2", "sub_ns")]
        
        # Use refresh=True to ensure the mock is called
        response = client.get("/api/namespaces?refresh=True")

    assert response.status_code == 200
    assert response.json() == [
        {"id": 0, "text": "ns1"},
        {"id": 1, "text": "ns2.sub_ns"}
    ], "The API response did not match the expected structure for namespaces."
    
    # Verify the endpoint modified our patched list in-place
    assert mock_namespaces_list == [("ns1",), ("ns2", "sub_ns")]
    mock_lv.get_namespaces.assert_called_once()

def test_read_tables_for_namespace(client: TestClient):
    """Test retrieving tables for a specific namespace."""
    # Simulate a logged-in user for this endpoint
    app.dependency_overrides[check_auth] = lambda: "test@example.com"
    
    with patch('app.api.tables.lv') as mock_lv:
        mock_lv.get_tables.return_value = [("ns1", "table_a"), ("ns1", "table_b")]
        
        response = client.get("/api/tables?namespace=ns1")

    assert response.status_code == 200
    # The endpoint logic extracts the last part of the table name
    assert response.json() == [
        {"id": 0, "text": "table_a", "namespace": "ns1"},
        {"id": 1, "text": "table_b", "namespace": "ns1"}
    ]
    mock_lv.get_tables.assert_called_once_with("ns1")

    # Clean up the dependency override
    app.dependency_overrides.clear()

# --- Table Detail Endpoint Tests ---

@patch('app.api.tables.lv')
@patch('app.api.tables.df_to_records')
def test_read_table_snapshots(mock_df_to_records, mock_lv, client: TestClient):
    """Test retrieving snapshot data for a table."""
    mock_table_obj = MagicMock()
    # Mock the `get_table` dependency to return our mock table
    app.dependency_overrides[get_table] = lambda: mock_table_obj

    # Mock the return value of the function that converts a DataFrame to JSON
    mock_df_to_records.return_value = [{"snapshot_id": 123, "records": 100}]
    
    response = client.get("/api/tables/ns1.table1/snapshots")

    assert response.status_code == 200
    assert response.json() == [{"snapshot_id": 123, "records": 100}]
    
    # Verify that the catalog was called with the mock table from the dependency
    mock_lv.get_snapshot_data.assert_called_once_with(mock_table_obj)
    mock_df_to_records.assert_called_once()

    app.dependency_overrides.clear()

@patch('app.api.tables.lv')
def test_read_schema_data(mock_lv, client: TestClient):
    """Test retrieving schema data for a table."""
    mock_table_obj = MagicMock()
    app.dependency_overrides[get_table] = lambda: mock_table_obj
    
    # Mock the underlying catalog and conversion functions
    with patch('app.api.tables.df_to_records') as mock_df_to_records:
        mock_df_to_records.return_value = [{"field": "id", "type": "long"}]
        response = client.get("/api/tables/ns1.table1/schema")

    assert response.status_code == 200
    assert response.json() == [{"field": "id", "type": "long"}]
    mock_lv.get_schema.assert_called_once_with(mock_table_obj)
    app.dependency_overrides.clear()

# --- Authorization Tests ---

def test_read_sample_data_authz_success(client: TestClient):
    """Test sample data endpoint when authorization succeeds."""
    mock_table_obj = MagicMock()
    app.dependency_overrides[get_table] = lambda: mock_table_obj

    # Patch the dependencies used within the endpoint
    with patch('app.api.tables.authz_') as mock_authz, \
         patch('app.api.tables.lv') as mock_lv, \
         patch('app.api.tables.df_to_records') as mock_df_to_records:
        
        # Simulate successful authorization check
        mock_authz.has_access.return_value = True
        mock_df_to_records.return_value = [{"col_a": 1, "col_b": "xyz"}]
        
        response = client.get("/api/tables/ns1.table1/sample")

    assert response.status_code == 200
    assert response.json() == [{"col_a": 1, "col_b": "xyz"}]
    mock_authz.has_access.assert_called_once()
    mock_lv.get_sample_data.assert_called_once()
    app.dependency_overrides.clear()


def test_read_partitions_authz_failure(client: TestClient):
    """Test partitions endpoint when authorization fails."""
    mock_table_obj = MagicMock()
    app.dependency_overrides[get_table] = lambda: mock_table_obj
    
    with patch('app.api.tables.authz_') as mock_authz:
        # Simulate failed authorization check
        # The `has_access` function in the code modifies the response directly
        # and returns None, so we replicate that behavior.
        def auth_fail(request, response, table_id):
            response.status_code = 403
            return None
        
        mock_authz.has_access.side_effect = auth_fail
        
        # We need to pass a dummy response object that the side_effect can modify
        from fastapi import Response
        response = client.get("/api/tables/ns1.table1/partitions", headers={"X-Dummy-Response": "true"})
    
    # The status code is set by the mock side_effect
    assert response.status_code == 403
    app.dependency_overrides.clear()

