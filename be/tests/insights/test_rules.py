# test_runner.py

from unittest.mock import MagicMock, patch
from types import SimpleNamespace
import pytest

from pyiceberg.types import (
    UUIDType,
    StructType,
    ListType,
    MapType,
    NestedField,
    StringType
)
from pyiceberg.schema import Schema

# Assuming the new runner and models are in these locations
from app.insights.runner import InsightsRunner
from app.models import Insight, InsightRun
from app.insights.rules import search_for_uuid_column


# This dictionary remains the source of truth for our tests
table_rules = {
    'namespace1.table1': ["SMALL_FILES", "NO_LOCATION"],
    'namespace1.table2': ["LARGE_FILES", "UUID_COLUMN"],
    'namespace1.table3': ["SMALL_FILES", "SMALL_FILES_LARGE_TABLE"],
    'namespace1.table4': ["NO_ROWS_TABLE"],
    'namespace1.table5': ["SMALL_FILES", "SNAPSHOT_SPRAWL_TABLE"],
    'namespace1.table6': ["SKEWED_OR_LARGEST_PARTITIONS_TABLE"]
}

# --- Fixtures and Mock Setup --------------------------------------------------

@pytest.fixture
def mock_storage_adapter():
    """Provides a mock storage adapter for the runner."""
    return MagicMock()

@pytest.fixture(autouse=True)
def mock_rules_fixture(monkeypatch):
    """
    Auto-used fixture to mock ALL_RULES_OBJECT for all tests in this file.
    It dynamically creates mock rules based on the `table_rules` dictionary.
    """
    # Invert the mapping for easier lookup inside the mock rule methods
    rule_to_tables = {}
    for table, rules in table_rules.items():
        for rule in rules:
            if rule not in rule_to_tables:
                rule_to_tables[rule] = []
            rule_to_tables[rule].append(table)

    all_rule_ids = list(rule_to_tables.keys())

    def create_mock_rule_method(rule_id):
        """Creates a mock method for a rule."""
        def mock_method(table):
            # The mock table's name is a callable MagicMock
            table_identifier = f"namespace1.{table.name()}"
            if table_identifier in rule_to_tables.get(rule_id, []):
                # If this rule should trigger, return a mock Insight object
                return Insight(code=rule_id, message=f"Message for {rule_id}", table=table, severity="", suggested_action="")
            return None  # Otherwise, return None
        return mock_method

    mock_rules_list = []
    for rule_id in all_rule_ids:
        mock_rule = MagicMock()
        mock_rule.id = rule_id
        mock_rule.method = create_mock_rule_method(rule_id)
        mock_rules_list.append(mock_rule)

    # Patch the global ALL_RULES_OBJECT that the runner will import and use
    monkeypatch.setattr("app.insights.runner.ALL_RULES_OBJECT", mock_rules_list)
    return mock_rules_list


# --- Mock Data Generation (Unchanged from original) --------------------------

def make_mock_table(name, file_count=200, file_size=50_000, location=None, schema=Schema(NestedField(field_id=1, name="field_1", field_type=StringType())), snapshots=5):
    mock_file = MagicMock()
    mock_file.file.file_size_in_bytes = file_size

    mock_scan = MagicMock()
    mock_scan.plan_files.return_value = [mock_file] * file_count

    mock_table = MagicMock()
    mock_table.name = MagicMock(return_value=name)
    mock_table.scan.return_value = mock_scan
    mock_table.location = location
    mock_table.schema.return_value = schema
    mock_table.history.return_value = [j for j in range(snapshots)]
    spec_mock = MagicMock()
    spec_mock.fields = None
    mock_table.spec.return_value = spec_mock
    mock_table.metadata.current_snapshot_id = None if file_count == 0 else 12345
    mock_pa_table = MagicMock()
    mock_pa_table.to_pydict.return_value = {
        'summary': [{'total-records': file_count * file_size}],
        'committed_at': [1678886400]
    }
    mock_table.inspect.snapshots.return_value.sort_by.return_value.select.return_value = mock_pa_table
    return mock_table

def make_task(path, fmt, records, size, partition_dict):
    partition = SimpleNamespace(**partition_dict)
    file = SimpleNamespace(
        file_path=path,
        file_format=fmt,
        record_count=records,
        file_size_in_bytes=size,
        partition=partition,
    )
    return SimpleNamespace(file=file)

def make_mock_partitioned_table(name, location = None):
    mock_table = MagicMock()
    mock_partition_spec = MagicMock()
    mock_partition_spec.fields = [MagicMock()]
    mock_table.spec.return_value = mock_partition_spec
    skewed_tasks = [
        make_task("f1", "parquet", 10, 100, {"category": "A"}),
        make_task("f2", "parquet", 100000, 1, {"category": "B"}),
        make_task("f3", "parquet", 1, 10000, {"category": "C"}),
    ]
    mock_scan_plan = MagicMock()
    mock_scan_plan.plan_files.return_value = skewed_tasks
    mock_table.scan.return_value = mock_scan_plan
    mock_table.name.return_value = name
    mock_table.location = location
    mock_table.metadata.current_snapshot_id = 12345
    mock_pa_table = MagicMock()
    mock_pa_table.to_pydict.return_value = {
        'summary': [{'total-records': 1210}],
        'committed_at': [1678886400]
    }
    mock_table.inspect.snapshots.return_value.sort_by.return_value.select.return_value = mock_pa_table
    spec_mock = MagicMock()
    spec_mock.fields = ("category",)
    mock_table.spec.return_value = spec_mock
    return mock_table

class MockLakeView:
    def __init__(self):
        self.tables = {
            "namespace1.table1": make_mock_table(name="table1", file_count=200, file_size=50_000, location=None),
            "namespace1.table2": make_mock_table(name="table2", file_count=10, file_size=1024**3, location="some_location", schema=Schema(NestedField(field_id=1, name="field_1", field_type=StringType()),NestedField(field_id=2, name="field_2", field_type=UUIDType()))),
            "namespace1.table3": make_mock_table(name="table3", file_count=1_200_000, file_size=49_000, location="some_location"),
            "namespace1.table4": make_mock_table(name="table4", file_count=0, file_size=0, location="some_location"),
            "namespace1.table5": make_mock_table(name="table5", file_count=1000, file_size=1, location="some_location", snapshots=1000),
            "namespace1.table6": make_mock_partitioned_table(name="table6", location="some_location"),
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
def test_run_for_table(mock_storage_adapter, table_id):
    """
    Tests running insights for a single table.
    Verifies the returned insights and the data saved to storage.
    """
    lakeview = MockLakeView()
    runner = InsightsRunner(lakeview, mock_storage_adapter)

    # Execute
    results = runner.run_for_table(table_id)

    # Verify returned results
    expected_codes = set(table_rules[table_id])
    result_codes = {r.code for r in results}
    assert result_codes == expected_codes

    # Verify storage call
    mock_storage_adapter.save.assert_called_once()
    saved_run = mock_storage_adapter.save.call_args[0][0]

    assert isinstance(saved_run, InsightRun)
    assert saved_run.namespace == "namespace1"
    assert saved_run.table_name == table_id.split('.')[1]

    saved_codes = {r.code for r in saved_run.results}
    assert saved_codes == expected_codes

def test_run_for_namespace(mock_storage_adapter):
    """
    Tests running insights for an entire namespace.
    Verifies that the runner correctly saves one InsightRun per table.
    """
    lakeview = MockLakeView()
    runner = InsightsRunner(lakeview, mock_storage_adapter)

    # Execute
    runner.run_for_namespace("namespace1")

    # Verify that save was called for each table in the namespace
    assert mock_storage_adapter.save.call_count == len(table_rules)

    # Extract the saved InsightRun objects and organize them by table for easy assertion
    saved_runs = [call.args[0] for call in mock_storage_adapter.save.call_args_list]
    results_from_storage = {
        f"{run.namespace}.{run.table_name}": {insight.code for insight in run.results}
        for run in saved_runs
    }

    # Assert that the saved data for each table matches the expected rules
    for table_id, expected_codes in table_rules.items():
        assert table_id in results_from_storage
        assert results_from_storage[table_id] == set(expected_codes)


def test_run_for_lakehouse(mock_storage_adapter):
    """
    Tests running insights for the entire lakehouse.
    Verifies that the runner correctly saves one InsightRun per table discovered.
    """
    lakeview = MockLakeView()
    runner = InsightsRunner(lakeview, mock_storage_adapter)

    # Execute
    runner.run_for_lakehouse()

    # Verify that save was called for each table in the lakehouse
    assert mock_storage_adapter.save.call_count == len(table_rules)

    # Extract the saved InsightRun objects
    saved_runs = [call.args[0] for call in mock_storage_adapter.save.call_args_list]
    results_from_storage = {
        f"{run.namespace}.{run.table_name}": {insight.code for insight in run.results}
        for run in saved_runs
    }

    # Assert that the saved data for each table matches the expected rules
    for table_id, expected_codes in table_rules.items():
        assert table_id in results_from_storage
        assert results_from_storage[table_id] == set(expected_codes)

# --- Standalone Rule Test (Unchanged) ----------------------------------------
# This test does not depend on the runner and is still valid.

@pytest.mark.parametrize("schema,expected", [
    (Schema(NestedField(field_id=1, name="field_1", field_type=StringType()),NestedField(field_id=2, name="field_2", field_type=UUIDType())), True),
    (Schema(NestedField(field_id=1, name="field_1", field_type=StringType()),NestedField(field_id=2, name="field_2", field_type=StringType())), False),
    (Schema(NestedField(field_id=1, name="field_1", field_type=StringType()),NestedField(field_id=2, name="field_2", field_type=ListType(element_id=3, element_type=StringType()))), False),
    (Schema(NestedField(field_id=1, name="field_1", field_type=StringType()),NestedField(field_id=2, name="field_2", field_type=ListType(element_id=3, element_type=UUIDType()))), True),
    (Schema(NestedField(field_id=1, name="field_1", field_type=StringType()),NestedField(field_id=2, name="field_2", field_type=ListType(element_id=3, element_type=StructType(fields=[NestedField(field_id=4, name="field_3",field_type=StringType())])))), False),
    (Schema(NestedField(field_id=1, name="field_1", field_type=StringType()),NestedField(field_id=2, name="field_2", field_type=ListType(element_id=3, element_type=StructType(fields=[NestedField(field_id=4, name="field_3",field_type=UUIDType())])))), True),
    (Schema(NestedField(field_id=1, name="field_1", field_type=StringType()),NestedField(field_id=2, name="field_2", field_type=StructType(fields=[NestedField(field_id=3, name="field_3",field_type=UUIDType())]))), True),
    (Schema(NestedField(field_id=1, name="field_1", field_type=StringType()),NestedField(field_id=2, name="field_2", field_type=StructType(fields=[NestedField(field_id=3, name="field_3",field_type=StringType())]))), False),
    (Schema(NestedField(field_id=1, name="field_1", field_type=StringType()),NestedField(field_id=2, name="field_2", field_type=MapType(key_id=3, key_type=StringType(),value_id=4, value_type=StringType()))), False),
    (Schema(NestedField(field_id=1, name="field_1", field_type=StringType()),NestedField(field_id=2, name="field_2", field_type=MapType(key_id=3, key_type=StringType(),value_id=4, value_type=UUIDType()))), True),
    (Schema(NestedField(field_id=1, name="field_1", field_type=StringType()),NestedField(field_id=2, name="field_2", field_type=MapType(key_id=3, key_type=UUIDType(),value_id=4, value_type=StringType()))), True),
    (Schema(NestedField(field_id=1, name="field_1", field_type=StringType()),NestedField(field_id=2, name="field_2", field_type=MapType(key_id=3, key_type=StringType(),value_id=4, value_type=StructType(fields=[NestedField(field_id=3, name="field_3",field_type=StringType())])))), False),
    (Schema(NestedField(field_id=1, name="field_1", field_type=StringType()),NestedField(field_id=2, name="field_2", field_type=MapType(key_id=3, key_type=StringType(),value_id=4, value_type=StructType(fields=[NestedField(field_id=3, name="field_3",field_type=UUIDType())])))), True)
])
def test_search_for_uuid_column(schema, expected):
    assert search_for_uuid_column(schema) == expected
