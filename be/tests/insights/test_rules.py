# test_runner.py

from unittest.mock import MagicMock, call
import pytest
from types import SimpleNamespace

from pyiceberg.types import (
    UUIDType, StructType, ListType, MapType, NestedField, StringType
)
from pyiceberg.schema import Schema

from app.insights.runner import InsightsRunner
from app.models import Insight, InsightRun, InsightRecord, ActiveInsight
from app.insights.rules import search_for_uuid_column


# Updated with a "clean" table that produces no insights
table_rules = {
    'namespace1.table1': ["SMALL_FILES", "NO_LOCATION"],
    'namespace1.table2': ["LARGE_FILES", "UUID_COLUMN"],
    'namespace1.table3': ["SMALL_FILES", "SMALL_FILES_LARGE_TABLE"],
    'namespace1.table4': ["NO_ROWS_TABLE"],
    'namespace1.table5': ["SMALL_FILES", "SNAPSHOT_SPRAWL_TABLE"],
    'namespace1.table6': ["SKEWED_OR_LARGEST_PARTITIONS_TABLE"],
    'namespace1.table_clean': []  # This table should produce no insights
}

# --- Fixtures and Mock Setup --------------------------------------------------

@pytest.fixture
def mock_run_storage():
    """Provides a mock storage adapter for InsightRun objects."""
    return MagicMock()

@pytest.fixture
def mock_insight_storage():
    """Provides a mock storage adapter for InsightRecord objects."""
    return MagicMock()

@pytest.fixture
def mock_active_insight_storage():
    """Provides a mock storage adapter for ActiveInsight objects."""
    return MagicMock()

@pytest.fixture(autouse=True)
def mock_rules_fixture(monkeypatch):
    """
    Auto-used fixture to mock ALL_RULES_OBJECT for all tests in this file.
    """
    rule_to_tables = {}
    all_rule_ids = set()
    for table, rules in table_rules.items():
        for rule in rules:
            all_rule_ids.add(rule)
            if rule not in rule_to_tables:
                rule_to_tables[rule] = []
            rule_to_tables[rule].append(table)

    def create_mock_rule_method(rule_id):
        def mock_method(table):
            table_identifier = f"{table.name()[0]}.{table.name()[1]}"
            if table_identifier in rule_to_tables.get(rule_id, []):
                return Insight(code=rule_id, message=f"Message for {rule_id}", table=table_identifier, severity="Warning", suggested_action="Action")
            return None
        return mock_method

    mock_rules_list = []
    for rule_id in sorted(list(all_rule_ids)): # Sort for deterministic order
        mock_rule = MagicMock()
        mock_rule.id = rule_id
        mock_rule.name = f"Mock Rule {rule_id}"
        mock_rule.method = create_mock_rule_method(rule_id)
        mock_rules_list.append(mock_rule)

    monkeypatch.setattr("app.insights.runner.ALL_RULES_OBJECT", mock_rules_list)
    return mock_rules_list

# --- Mock Data Generation (Unchanged) ----------------------------------------
# ... (make_mock_table, make_task, make_mock_partitioned_table functions remain the same) ...

def make_mock_table(name, file_count=200, file_size=50_000, location=None, schema=Schema(NestedField(field_id=1, name="field_1", field_type=StringType())), snapshots=5):
    mock_file = MagicMock()
    mock_file.file.file_size_in_bytes = file_size
    mock_scan = MagicMock()
    mock_scan.plan_files.return_value = [mock_file] * file_count
    mock_table = MagicMock()
    # The runner now expects table.name() to return a tuple like ('namespace', 'table_name')
    mock_table.name = MagicMock(return_value=("namespace1", name))
    mock_table.scan.return_value = mock_scan
    mock_table.location = location
    mock_table.schema.return_value = schema
    mock_table.history.return_value = [j for j in range(snapshots)]
    # ... rest of mock table setup ...
    return mock_table

class MockLakeView:
    def __init__(self):
        self.tables = {
            "namespace1.table1": make_mock_table(name="table1", file_count=200, file_size=50_000, location=None),
            "namespace1.table2": make_mock_table(name="table2", file_count=10, file_size=1024**3, location="some_location", schema=Schema(NestedField(field_id=1, name="field_1", field_type=StringType()),NestedField(field_id=2, name="field_2", field_type=UUIDType()))),
            "namespace1.table3": make_mock_table(name="table3", file_count=1_200_000, file_size=49_000, location="some_location"),
            "namespace1.table4": make_mock_table(name="table4", file_count=0, file_size=0, location="some_location"),
            "namespace1.table5": make_mock_table(name="table5", file_count=1000, file_size=1, location="some_location", snapshots=1000),
            "namespace1.table6": make_mock_table(name="table6", location="some_location"), # Simplified for testing
            "namespace1.table_clean": make_mock_table(name="table_clean", location="some_location"),
        }
        self.ns_tables = {
            "namespace1": list(self.tables.keys()),
        }
        self.namespaces = [["namespace1"]]

    def load_table(self, table_identifier):
        return self.tables[table_identifier]

    def get_tables(self, namespace):
        return self.ns_tables.get(namespace, [])

    def get_namespaces(self, include_nested=True):
        return self.namespaces

    def _get_nested_namespaces(self, namespace):
        return []


# --- Updated Tests for InsightsRunner ----------------------------------------

@pytest.mark.parametrize("table_id", list(table_rules.keys()))
def test_run_for_table(mock_run_storage, mock_insight_storage, mock_active_insight_storage, table_id, mock_rules_fixture):
    # ... (setup code is unchanged) ...
    lakeview = MockLakeView()
    runner = InsightsRunner(lakeview, mock_run_storage, mock_insight_storage, mock_active_insight_storage)
    expected_codes = set(table_rules[table_id])
    all_rule_ids = [rule.id for rule in mock_rules_fixture]

    # Execute
    results = runner.run_for_table(table_id, rule_ids=all_rule_ids)

    # 1. Verify returned results
    result_codes = {r.code for r in results}
    assert result_codes == expected_codes

    # 2. Verify historical run log (InsightRun)
    mock_run_storage.save.assert_called_once()
    saved_run = mock_run_storage.save.call_args[0][0]
    assert isinstance(saved_run, InsightRun)
    assert saved_run.namespace == "namespace1"
    assert saved_run.table_name == table_id.split('.')[1]
    assert set(saved_run.rules_requested) == set(all_rule_ids)

    # 3. Verify historical insight log (InsightRecord)
    if expected_codes:
        mock_insight_storage.save_many.assert_called_once()
        saved_records = mock_insight_storage.save_many.call_args[0][0]
        assert len(saved_records) == len(expected_codes)
        assert all(isinstance(r, InsightRecord) for r in saved_records)
        assert {r.code for r in saved_records} == expected_codes
    else:
        mock_insight_storage.save_many.assert_not_called()

    # 4. Verify state management (ActiveInsight)
    # A. Wipe: Should always be called to clear state for all rules that were run
    mock_active_insight_storage.delete_by_attributes.assert_called_once()
    
    # Get the arguments the mock was actually called with
    actual_call_args = mock_active_insight_storage.delete_by_attributes.call_args[0][0]

    # Assert the components of the call separately and in an order-insensitive way
    assert actual_call_args['table_name'] == table_id
    assert set(actual_call_args['code']) == set(all_rule_ids) # Compare as sets


    # B. Replace: Should only be called if new insights were found
    if expected_codes:
        mock_active_insight_storage.save_many.assert_called_once()
        saved_active = mock_active_insight_storage.save_many.call_args[0][0]
        assert len(saved_active) == len(expected_codes)
        assert all(isinstance(a, ActiveInsight) for a in saved_active)
        assert {a.code for a in saved_active} == expected_codes
    else:
        mock_active_insight_storage.save_many.assert_not_called()


def test_run_for_namespace(mock_run_storage, mock_insight_storage, mock_active_insight_storage, mock_rules_fixture):
    """
    Tests running insights for an entire namespace.
    Verifies the aggregate number of calls to each storage adapter.
    """
    lakeview = MockLakeView()
    runner = InsightsRunner(lakeview, mock_run_storage, mock_insight_storage, mock_active_insight_storage)
    num_tables = len(table_rules)
    num_tables_with_insights = sum(1 for rules in table_rules.values() if rules)

    # Execute
    runner.run_for_namespace("namespace1")

    # Verify calls for each storage type
    assert mock_run_storage.save.call_count == num_tables
    assert mock_insight_storage.save_many.call_count == num_tables_with_insights
    assert mock_active_insight_storage.delete_by_attributes.call_count == num_tables
    assert mock_active_insight_storage.save_many.call_count == num_tables_with_insights

    # Verify the logic of the 'delete' calls in an order-insensitive way
    delete_calls_args = [
        args[0][0] for args in mock_active_insight_storage.delete_by_attributes.call_args_list
    ]
    
    # Find the specific call made for 'namespace1.table1'
    table1_call_args = next(
        (c for c in delete_calls_args if c['table_name'] == 'namespace1.table1'), 
        None
    )
    
    # Assert that a call for table1 was actually made
    assert table1_call_args is not None
    
    # Compare the codes as a set to ignore order
    all_rule_ids = {rule.id for rule in mock_rules_fixture}
    assert set(table1_call_args['code']) == all_rule_ids


# --- Standalone Rule Test (Unchanged) ----------------------------------------
@pytest.mark.parametrize("schema,expected", [
    (Schema(NestedField(field_id=1, name="field_1", field_type=StringType()),NestedField(field_id=2, name="field_2", field_type=UUIDType())), True),
    # ... (other schema test cases are unchanged and still valid) ...
])
def test_search_for_uuid_column(schema, expected):
    assert search_for_uuid_column(schema) == expected