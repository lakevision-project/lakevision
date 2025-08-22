from unittest.mock import MagicMock
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
from app.insights.rules import search_for_uuid_column
from pyiceberg.partitioning import PartitionSpec

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

    # Set a current_snapshot_id to simulate a non-empty table
    mock_table.metadata.current_snapshot_id = None if file_count == 0 else 12345
    # Define the return value for the inspect.snapshots() method when not empty
    mock_pa_table = MagicMock()
    mock_pa_table.to_pydict.return_value = {
        'summary': [{'total-records': file_count * file_size}],
        'committed_at': [1678886400]
    }
    # Chain the mock methods: inspect.snapshots().sort_by().select()
    mock_table.inspect.snapshots.return_value.sort_by.return_value.select.return_value = mock_pa_table

    return mock_table
    


class MockLakeView:
    def __init__(self):
        self.tables = {
            "namespace1.table1": make_mock_table(name="table1", file_count=200, file_size=50_000, location=None),
            "namespace1.table2": make_mock_table(name="table2", file_count=10, file_size=1024**3, location="some_location", schema=Schema(NestedField(field_id=1, name="field_1", field_type=StringType()),NestedField(field_id=2, name="field_2", field_type=UUIDType()))),
            "namespace1.table3": make_mock_table(name="table3", file_count=1_200_000, file_size=49_000, location="some_location"),
            "namespace1.table4": make_mock_table(name="table4", file_count=0, file_size=0, location="some_location"),
            "namespace1.table5": make_mock_table(name="table5", file_count=1000, file_size=1, location="some_location", snapshots=1000),
        }
        self.ns_tables = {
            "namespace1": ["namespace1.table1", "namespace1.table2", "namespace1.table3", "namespace1.table4", "namespace1.table5"],
        }
        self.namespaces = [["namespace1"]]

    def load_table(self, table_identifier):
        return self.tables[table_identifier]

    def get_tables(self, namespace):
        return self.ns_tables[namespace]

    def get_namespaces(self, include_nested=True):
        return self.namespaces

    def _get_nested_namespaces(self, namespace):
        # For simplicity in this mock, return empty (no nested)
        return []

table_rules = {
    'namespace1.table1':["SMALL_FILES", "NO_LOCATION"],
    'namespace1.table2':["LARGE_FILES", "UUID_COLUMN"], 
    'namespace1.table3':["SMALL_FILES", "SMALL_FILES_LARGE_TABLE"],
    'namespace1.table4':["NO_ROWS_TABLE"],
    'namespace1.table5':["SMALL_FILES","SNAPSHOT_SPRAWL_TABLE"]
}


@pytest.mark.parametrize("table", [
        ('namespace1.table1'), 
        ('namespace1.table2'), 
        ('namespace1.table3'),
        ('namespace1.table4'),
        ('namespace1.table5')
    ])
def test_run_for_table(table):
    lakeview = MockLakeView()
    runner = InsightsRunner(lakeview)
    results = runner.run_for_table(table)
    codes = {r.code for r in results}
    assert set(codes) == set(table_rules[table])

def test_run_for_namespace():
    lakeview = MockLakeView()
    runner = InsightsRunner(lakeview)
    ns_results = runner.run_for_namespace("namespace1")
    assert "namespace1.table1" in ns_results
    assert "namespace1.table2" in ns_results
    assert "namespace1.table3" in ns_results
    assert "namespace1.table4" in ns_results
    assert "namespace1.table5" in ns_results
    codes1 = {r.code for r in ns_results["namespace1.table1"]}
    codes2 = {r.code for r in ns_results["namespace1.table2"]}
    codes3 = {r.code for r in ns_results["namespace1.table3"]}
    codes4 = {r.code for r in ns_results["namespace1.table4"]}
    codes5 = {r.code for r in ns_results["namespace1.table5"]}
    assert set(codes1) == set(table_rules["namespace1.table1"])
    assert set(codes2) == set(table_rules["namespace1.table2"])
    assert set(codes3) == set(table_rules["namespace1.table3"])
    assert set(codes4) == set(table_rules["namespace1.table4"])
    assert set(codes5) == set(table_rules["namespace1.table5"])

def test_run_for_lakehouse():
    lakeview = MockLakeView()
    runner = InsightsRunner(lakeview)
    all_results = runner.run_for_lakehouse()
    assert "namespace1.table1" in all_results
    assert "namespace1.table2" in all_results
    assert "namespace1.table3" in all_results
    assert "namespace1.table4" in all_results
    assert "namespace1.table5" in all_results
    codes1 = [r.code for r in all_results["namespace1.table1"]]
    codes2 = [r.code for r in all_results["namespace1.table2"]]
    codes3 = [r.code for r in all_results["namespace1.table3"]]
    codes4 = {r.code for r in all_results["namespace1.table4"]}
    codes5 = {r.code for r in all_results["namespace1.table5"]}
    assert set(codes1) == set(table_rules["namespace1.table1"])
    assert set(codes2) == set(table_rules["namespace1.table2"])
    assert set(codes3) == set(table_rules["namespace1.table3"])
    assert set(codes4) == set(table_rules["namespace1.table4"])
    assert set(codes5) == set(table_rules["namespace1.table5"])


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
def test_search_for_uuid_column(schema,expected): 
    assert search_for_uuid_column(schema) == expected
