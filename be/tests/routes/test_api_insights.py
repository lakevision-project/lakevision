import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from datetime import datetime, timezone

from app.api import app
from app.dependencies import get_runner
from app.models import RuleOut, InsightRunOut, RuleSummaryOut, InsightOccurrence

# Mock runner to be used in tests
mock_runner = MagicMock()

# Use FastAPI's dependency overrides to replace the real runner with our mock
app.dependency_overrides[get_runner] = lambda: mock_runner

@pytest.fixture(autouse=True)
def reset_mock_runner():
    """Reset the mock runner before each test."""
    mock_runner.reset_mock()

def test_get_latest_table_insights_for_namespace(client: TestClient):
    """Test fetching insights for a whole namespace."""
    # FIX: Create complete mock objects to pass Pydantic validation
    mock_run_data = {
        "id": "run1", "namespace": "ns1", "table_name": "table1", 
        "run_type": "manual", "status": "complete", 
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "rules_requested": ["ALL"], "results": []
    }
    mock_runner.get_latest_run.return_value = [InsightRunOut(**mock_run_data)]
    
    response = client.get("/api/namespaces/ns1/insights?size=1")
    
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]['id'] == "run1"
    mock_runner.get_latest_run.assert_called_once_with(
        namespace="ns1", table_name=None, size=1, showEmpty=True
    )

def test_get_latest_table_insights_for_table(client: TestClient):
    """Test fetching insights for a specific table."""
    mock_run_data = {
        "id": "run3", "namespace": "ns1", "table_name": "tableA",
        "run_type": "manual", "status": "complete",
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "rules_requested": ["ALL"], "results": []
    }
    mock_runner.get_latest_run.return_value = [InsightRunOut(**mock_run_data)]
    
    response = client.get("/api/namespaces/ns1/tableA/insights?size=1&showEmpty=false")
    
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]['id'] == "run3"
    mock_runner.get_latest_run.assert_called_once_with(
        namespace="ns1", table_name="tableA", size=1, showEmpty=False
    )

def test_get_insights_summary_for_namespace(client: TestClient):
    """Test getting a summary for a namespace."""
    mock_summary_data = {
        "code": "RULE_A", "namespace": "ns1", "suggested_action": "Action A",
        "occurrences": [
            InsightOccurrence(table_name="table1", severity="LOW", message="msg", timestamp=datetime.now(timezone.utc))
        ]
    }
    mock_runner.get_summary_by_rule.return_value = [RuleSummaryOut(**mock_summary_data)]

    response = client.get("/api/namespaces/ns1/insights/summary")

    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]['code'] == "RULE_A"
    mock_runner.get_summary_by_rule.assert_called_once_with(
        namespace="ns1", table_name=None, rule_codes=None
    )

def test_get_insights_summary_for_table_with_rules_filter(client: TestClient):
    """Test getting a summary for a table with a rule filter."""
    mock_runner.get_summary_by_rule.return_value = []

    response = client.get("/api/namespaces/ns1/tableB/insights/summary?rules=RULE_A&rules=RULE_B")

    assert response.status_code == 200
    mock_runner.get_summary_by_rule.assert_called_once_with(
        namespace="ns1", table_name="tableB", rule_codes=['RULE_A', 'RULE_B']
    )

@patch('app.api.insights.ALL_RULES_OBJECT')
def test_get_insight_rules(mock_all_rules, client: TestClient):
    """Test the endpoint that returns all available insight rules."""
    # The real object has a `method` attribute that can't be None.
    # We mock it with a simple function.
    mock_rule = MagicMock()
    mock_rule.id = "RULE_A"
    mock_rule.name = "Rule A"
    mock_rule.description = "Desc A"
    mock_rule.method = lambda: "foo"
    
    mock_all_rules.__iter__.return_value = [mock_rule]

    response = client.get("/api/lakehouse/insights/rules")
    
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]['id'] == 'RULE_A'

