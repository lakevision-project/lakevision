# tests/insights/test_rules.py

from unittest.mock import MagicMock, patch
from app.insights.runner import InsightsRunner
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
# Import the real rules file to test the standalone function
from app.insights.rules import search_for_uuid_column
# Import the utility function that the runner uses
from app.insights.utils import get_namespace_and_table_name
# Import all the models we need to mock and verify
from app.models import Insight, InsightRun, InsightRecord, ActiveInsight, InsightRunOut, RuleSummaryOut, InsightOccurrence
from types import SimpleNamespace

# --- Fixtures -----------------------------------------------------------------

@pytest.fixture
def run_storage_mock():
    """Mocks the storage adapter for InsightRun objects."""
    # FIX: Explicitly set the table_name attribute
    mock = MagicMock()
    mock.table_name = "insight_run_table" 
    return mock

@pytest.fixture
def insight_storage_mock():
    """Mocks the storage adapter for InsightRecord objects."""
    # FIX: Explicitly set the table_name attribute
    mock = MagicMock()
    mock.table_name = "insight_record_table"
    return mock

@pytest.fixture
def active_insight_storage_mock():
    """Mocks the storage adapter for ActiveInsight objects."""
    return MagicMock()

# --- Mock Data Generation -----------------------------------------------------
# (This section is unchanged)

def make_mock_table(name, file_count=200, file_size=50_000, location=None, schema=Schema(NestedField(field_id=1, name="field_1", field_type=StringType())), snapshots = 5):
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
            "namespace1": ["namespace1.table1", "namespace1.table2", "namespace1.table3", "namespace1.table4", "namespace1.body.table5", "namespace1.table6"],
        }
        self.namespaces = [["namespace1"]]

    def load_table(self, table_identifier):
        if table_identifier == "namespace1.body.table5":
            table_identifier = "namespace1.table5"
        return self.tables[table_identifier]

    def get_tables(self, namespace):
        return ["namespace1.table1", "namespace1.table2", "namespace1.table3", "namespace1.table4", "namespace1.table5", "namespace1.table6"]

    def get_namespaces(self, include_nested=True):
        return self.namespaces

    def _get_nested_namespaces(self, namespace):
        return []

table_rules = {
    'namespace1.table1':["SMALL_FILES", "NO_LOCATION"],
    'namespace1.table2':["LARGE_FILES", "UUID_COLUMN"],
    'namespace1.table3':["SMALL_FILES", "SMALL_FILES_LARGE_TABLE"],
    'namespace1.table4':["NO_ROWS_TABLE"],
    'namespace1.table5':["SMALL_FILES","SNAPSHOT_SPRAWL_TABLE"],
    'namespace1.table6':["SKEWED_OR_LARGEST_PARTITIONS_TABLE"]
}

# This section creates a dynamic mock of ALL_RULES_OBJECT based on the table_rules dictionary.
all_rule_ids = set(code for codes in table_rules.values() for code in codes)
mock_rules_list = []
for rule_id in all_rule_ids:
    def method_factory(current_rule_id):
        def mock_method(table):
            table_identifier = f"namespace1.{table.name()}"
            if table_identifier in table_rules and current_rule_id in table_rules[table_identifier]:
                # This matches the new rules.py: `table` is a string
                return Insight(
                    table=table_identifier,
                    code=current_rule_id,
                    message="A mock message",
                    severity="LOW",
                    suggested_action="Do something."
                )
            return None
        return mock_method

    rule = MagicMock()
    rule.id = rule_id
    rule.method = method_factory(rule_id)
    mock_rules_list.append(rule)

# Helper function to create a mock runner.
def create_mock_runner(lakeview, run_storage_mock, insight_storage_mock, active_insight_storage_mock):
    # We patch the __init__ to avoid needing a real config object
    with patch.object(InsightsRunner, "__init__", lambda s, lv, rs, is_, ais: None):
        runner = InsightsRunner(None, None, None, None)
        runner.lakeview = lakeview
        runner.run_storage = run_storage_mock
        runner.insight_storage = insight_storage_mock
        runner.active_insight_storage = active_insight_storage_mock
    return runner

# --- Existing Tests -----------------------------------------------------------

@pytest.mark.parametrize("table", [
        ('namespace1.table1'),
        ('namespace1.table2'),
        ('namespace1.table3'),
        ('namespace1.table4'),
        ('namespace1.table5'),
        ('namespace1.table6')
    ])
def test_run_for_table(run_storage_mock, insight_storage_mock, active_insight_storage_mock, table):
    lakeview = MockLakeView()
    runner = create_mock_runner(lakeview, run_storage_mock, insight_storage_mock, active_insight_storage_mock)
    
    with patch("app.insights.runner.ALL_RULES_OBJECT", mock_rules_list):
        with patch("app.insights.runner.get_namespace_and_table_name", return_value=("namespace1", table.split('.')[1])):
            results = runner.run_for_table(table)
    
    codes = {r.code for r in results}
    expected_codes = set(table_rules[table])
    assert set(codes) == expected_codes
    run_storage_mock.save.assert_called_once()
    saved_run = run_storage_mock.save.call_args[0][0] 
    assert isinstance(saved_run, InsightRun)
    assert set(saved_run.rules_requested) == all_rule_ids
    active_insight_storage_mock.delete_by_attributes.assert_called_once()
    delete_attrs = active_insight_storage_mock.delete_by_attributes.call_args[0][0]
    assert set(delete_attrs["code"]) == all_rule_ids

    expected_insight_count = len(expected_codes)
    assert expected_insight_count > 0
    insight_storage_mock.save_many.assert_called_once()
    saved_records = insight_storage_mock.save_many.call_args[0][0]
    assert len(saved_records) == expected_insight_count
    assert saved_records[0].run_id == saved_run.id
    active_insight_storage_mock.save_many.assert_called_once()
    saved_active = active_insight_storage_mock.save_many.call_args[0][0]
    assert len(saved_active) == expected_insight_count
    assert saved_active[0].last_seen_run_id == saved_run.id

def test_run_for_namespace(run_storage_mock, insight_storage_mock, active_insight_storage_mock):
    lakeview = MockLakeView()
    runner = create_mock_runner(lakeview, run_storage_mock, insight_storage_mock, active_insight_storage_mock)
    table_names = [t.split('.')[-1] for t in lakeview.get_tables("namespace1")]
    mock_name_tuples = [("namespace1", name) for name in table_names]
    with patch("app.insights.runner.ALL_RULES_OBJECT", mock_rules_list):
        with patch("app.insights.runner.get_namespace_and_table_name", side_effect=mock_name_tuples):
            runner.run_for_namespace("namespace1")
    assert run_storage_mock.save.call_count == 6
    assert active_insight_storage_mock.delete_by_attributes.call_count == 6
    assert insight_storage_mock.save_many.call_count == 6
    assert active_insight_storage_mock.save_many.call_count == 6
    saved_runs = [call.args[0] for call in run_storage_mock.save.call_args_list]
    results_from_storage = {f"{run.namespace}.{run.table_name}": run for run in saved_runs}
    assert "namespace1.table1" in results_from_storage
    assert "namespace1.table6" in results_from_storage

def test_run_for_lakehouse(run_storage_mock, insight_storage_mock, active_insight_storage_mock):
    lakeview = MockLakeView()
    runner = create_mock_runner(lakeview, run_storage_mock, insight_storage_mock, active_insight_storage_mock)
    table_names = [t.split('.')[-1] for t in lakeview.get_tables("namespace1")]
    mock_name_tuples = [("namespace1", name) for name in table_names]
    with patch("app.insights.runner.ALL_RULES_OBJECT", mock_rules_list):
        with patch("app.insights.runner.get_namespace_and_table_name", side_effect=mock_name_tuples):
            runner.run_for_lakehouse()
    assert run_storage_mock.save.call_count == 6
    assert active_insight_storage_mock.delete_by_attributes.call_count == 6
    assert insight_storage_mock.save_many.call_count == 6
    assert active_insight_storage_mock.save_many.call_count == 6
    saved_runs = [call.args[0] for call in run_storage_mock.save.call_args_list]
    results_from_storage = {f"{run.namespace}.{run.table_name}": run for run in saved_runs}
    assert "namespace1.table1" in results_from_storage
    assert "namespace1.table6" in results_from_storage

@pytest.mark.parametrize("schema,expected", [
        (Schema(NestedField(field_id=1, name="field_1", field_type=StringType()),NestedField(field_id=2, name="field_2", field_type=UUIDType())), True),
        (Schema(NestedField(field_id=1, name="field_1", field_type=StringType()),NestedField(field_id=2, name="field_2", field_type=StringType())), False),
        (Schema(NestedField(field_id=1, name="field_1", field_type=StringType()),NestedField(field_id=2, name="field_2", field_type=ListType(element_id=3, element_type=UUIDType()))), True),
        (Schema(NestedField(field_id=1, name="field_1", field_type=StringType()),NestedField(field_id=2, name="field_2", field_type=StructType(fields=[NestedField(field_id=3, name="field_3",field_type=UUIDType())]))), True),
        (Schema(NestedField(field_id=1, name="field_1", field_type=StringType()),NestedField(field_id=2, name="field_2", field_type=MapType(key_id=3, key_type=StringType(),value_id=4, value_type=UUIDType()))), True),
        (Schema(NestedField(field_id=1, name="field_1", field_type=StringType()),NestedField(field_id=2, name="field_2", field_type=MapType(key_id=3, key_type=UUIDType(),value_id=4, value_type=StringType()))), True),
])
def test_search_for_uuid_column(schema,expected):
    assert search_for_uuid_column(schema) == expected

# --- New Tests for Runner Features ------------------------------------------

def test_run_for_table_with_specific_rule_ids(run_storage_mock, insight_storage_mock, active_insight_storage_mock):
    """
    Tests that the runner correctly filters when a `rule_ids` list is provided.
    """
    lakeview = MockLakeView()
    runner = create_mock_runner(lakeview, run_storage_mock, insight_storage_mock, active_insight_storage_mock)
    
    table_id = 'namespace1.table1'
    requested_rule_ids = ['SMALL_FILES', 'UUID_COLUMN']
    
    with patch("app.insights.runner.ALL_RULES_OBJECT", mock_rules_list):
        with patch("app.insights.runner.get_namespace_and_table_name", return_value=("namespace1", "table1")):
            results = runner.run_for_table(table_id, rule_ids=requested_rule_ids)

    result_codes = {r.code for r in results}
    assert result_codes == {'SMALL_FILES'} 
    run_storage_mock.save.assert_called_once()
    saved_run = run_storage_mock.save.call_args[0][0]
    assert set(saved_run.rules_requested) == set(requested_rule_ids)
    active_insight_storage_mock.delete_by_attributes.assert_called_once()
    delete_attrs = active_insight_storage_mock.delete_by_attributes.call_args[0][0]
    assert set(delete_attrs["code"]) == set(requested_rule_ids)
    insight_storage_mock.save_many.assert_called_once()
    saved_records = insight_storage_mock.save_many.call_args[0][0]
    assert len(saved_records) == 1
    assert saved_records[0].code == 'SMALL_FILES'
    active_insight_storage_mock.save_many.assert_called_once()
    saved_active = active_insight_storage_mock.save_many.call_args[0][0]
    assert len(saved_active) == 1
    assert saved_active[0].code == 'SMALL_FILES'

def test_run_for_table_with_invalid_rule_id(run_storage_mock, insight_storage_mock, active_insight_storage_mock):
    """
    Tests that the runner raises a ValueError for an unknown rule ID.
    """
    lakeview = MockLakeView()
    runner = create_mock_runner(lakeview, run_storage_mock, insight_storage_mock, active_insight_storage_mock)
    
    with patch("app.insights.runner.ALL_RULES_OBJECT", mock_rules_list):
        with pytest.raises(ValueError) as e:
            runner.run_for_table('namespace1.table1', rule_ids=['INVALID_RULE_ID', 'SMALL_FILES'])
    
    assert "Invalid rule IDs provided: INVALID_RULE_ID" in str(e.value)
    run_storage_mock.save.assert_not_called()
    insight_storage_mock.save_many.assert_not_called()
    active_insight_storage_mock.delete_by_attributes.assert_not_called()

def test_get_latest_run_no_data(run_storage_mock, insight_storage_mock, active_insight_storage_mock):
    """
    Tests the get_latest_run method when no runs are found.
    """
    lakeview = MockLakeView()
    runner = create_mock_runner(lakeview, run_storage_mock, insight_storage_mock, active_insight_storage_mock)
    
    run_storage_mock.find_by_raw_query.return_value = []
    
    results = runner.get_latest_run(namespace="namespace1", size=10)
    
    assert results == []
    run_storage_mock.find_by_raw_query.assert_called_once()
    insight_storage_mock.get_by_attributes.assert_not_called()

def test_get_latest_run_with_data(run_storage_mock, insight_storage_mock, active_insight_storage_mock):
    """
    Tests the get_latest_run method, checking its join logic.
    """
    lakeview = MockLakeView()
    runner = create_mock_runner(lakeview, run_storage_mock, insight_storage_mock, active_insight_storage_mock)

    mock_run_1 = InsightRun(id="run_id_1", namespace="ns1", table_name="table1", run_type="manual", rules_requested=[])
    mock_run_2 = InsightRun(id="run_id_2", namespace="ns1", table_name="table2", run_type="manual", rules_requested=[])
    run_storage_mock.find_by_raw_query.return_value = [mock_run_1, mock_run_2]

    mock_record_1 = InsightRecord(run_id="run_id_1", code="SMALL_FILES", message="...", severity="LOW", table="ns1.table1", suggested_action="Action A")
    mock_record_2 = InsightRecord(run_id="run_id_1", code="NO_LOCATION", message="...", severity="MED", table="ns1.table1", suggested_action="Action B")
    insight_storage_mock.get_by_attributes.return_value = [mock_record_1, mock_record_2]
    
    results = runner.get_latest_run(namespace="ns1", size=10)

    # 3. Check the query and filtering
    # FIX: Check for the real table name from the mock fixture
    run_storage_mock.find_by_raw_query.assert_called_once_with(
        'SELECT * FROM "insight_run_table" WHERE "insight_run_table"."namespace" = :namespace ORDER BY run_timestamp DESC LIMIT :limit',
        {'namespace': 'ns1', 'limit': 10}
    )
    insight_storage_mock.get_by_attributes.assert_called_once_with({"run_id": ["run_id_1", "run_id_2"]})

    # 4. Check the joined results
    assert len(results) == 2
    assert isinstance(results[0], InsightRunOut)
    assert results[0].id == "run_id_1"
    assert len(results[0].results) == 2
    assert {r.code for r in results[0].results} == {"SMALL_FILES", "NO_LOCATION"}
    
    assert results[1].id == "run_id_2"
    assert len(results[1].results) == 0

def test_get_latest_run_with_data_show_empty_false(run_storage_mock, insight_storage_mock, active_insight_storage_mock):
    """
    Tests the get_latest_run logic when showEmpty=False.
    """
    lakeview = MockLakeView()
    runner = create_mock_runner(lakeview, run_storage_mock, insight_storage_mock, active_insight_storage_mock)

    mock_run_1 = InsightRun(id="run_id_1", namespace="ns1", table_name="table1", run_type="manual", rules_requested=[])
    run_storage_mock.find_by_raw_query.return_value = [mock_run_1]
    mock_record_1 = InsightRecord(run_id="run_id_1", code="SMALL_FILES", message="...", severity="LOW", table="ns1.table1", suggested_action="Action A")
    insight_storage_mock.get_by_attributes.return_value = [mock_record_1]

    results = runner.get_latest_run(namespace="ns1", size=10, showEmpty=False)

    # Check that the raw query contains the EXISTS clause
    run_storage_mock.find_by_raw_query.assert_called_once()
    query_string = run_storage_mock.find_by_raw_query.call_args[0][0]
    
    # FIX: Check for the real table names from the mock fixtures
    expected_clause = 'EXISTS (SELECT 1 FROM "insight_record_table" WHERE "insight_record_table"."run_id" = "insight_run_table"."id")'
    assert expected_clause in query_string

    assert len(results) == 1
    assert results[0].id == "run_id_1"
    assert len(results[0].results) == 1
    assert results[0].results[0].code == "SMALL_FILES"


def test_get_summary_by_rule_no_data(run_storage_mock, insight_storage_mock, active_insight_storage_mock):
    """
    Tests the get_summary_by_rule method when no active insights are found.
    """
    lakeview = MockLakeView()
    runner = create_mock_runner(lakeview, run_storage_mock, insight_storage_mock, active_insight_storage_mock)
    
    active_insight_storage_mock.get_by_attributes.return_value = []
    
    results = runner.get_summary_by_rule(namespace="namespace1")
    
    assert results == []
    active_insight_storage_mock.get_by_attributes.assert_called_once_with({'namespace': 'namespace1'})

def test_get_summary_by_rule_with_grouping(run_storage_mock, insight_storage_mock, active_insight_storage_mock):
    """
    Tests the get_summary_by_rule method, checking its grouping logic.
    """
    lakeview = MockLakeView()
    runner = create_mock_runner(lakeview, run_storage_mock, insight_storage_mock, active_insight_storage_mock)

    # 1. Mock ActiveInsight data
    active_1 = ActiveInsight(namespace="ns1", table_name="table1", code="SMALL_FILES", message="msg1", severity="LOW", suggested_action="Action A", last_seen_run_id="r1")
    active_2 = ActiveInsight(namespace="ns1", table_name="table2", code="SMALL_FILES", message="msg2", severity="LOW", suggested_action="Action A", last_seen_run_id="r2")
    active_3 = ActiveInsight(namespace="ns1", table_name="table3", code="LARGE_FILES", message="msg3", severity="HIGH", suggested_action="Action B", last_seen_run_id="r3")
    active_4 = ActiveInsight(namespace="ns2", table_name="table4", code="SMALL_FILES", message="msg4", severity="LOW", suggested_action="Action A", last_seen_run_id="r4")
    
    active_insight_storage_mock.get_by_attributes.return_value = [active_1, active_2, active_3]
    
    # 2. Call for "ns1"
    results = runner.get_summary_by_rule(namespace="ns1")
    
    # 3. Check filtering
    active_insight_storage_mock.get_by_attributes.assert_called_once_with({'namespace': 'ns1'})
    
    # 4. Check grouping
    assert len(results) == 2 # One group for SMALL_FILES, one for LARGE_FILES
    assert isinstance(results[0], RuleSummaryOut)
    
    # Sort results to make assertions deterministic
    results.sort(key=lambda x: x.code)
    
    large_files_summary = results[0]
    small_files_summary = results[1]

    assert large_files_summary.code == "LARGE_FILES"
    assert large_files_summary.namespace == "ns1"
    assert large_files_summary.suggested_action == "Action B"
    assert len(large_files_summary.occurrences) == 1
    assert large_files_summary.occurrences[0].table_name == "table3"
    
    assert small_files_summary.code == "SMALL_FILES"
    assert small_files_summary.namespace == "ns1"
    assert small_files_summary.suggested_action == "Action A"
    assert len(small_files_summary.occurrences) == 2
    assert {o.table_name for o in small_files_summary.occurrences} == {"table1", "table2"}
    assert isinstance(small_files_summary.occurrences[0], InsightOccurrence)