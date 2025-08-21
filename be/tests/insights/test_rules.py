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

def make_mock_table(name, file_count=200, file_size=50_000, location=None):
    mock_file = MagicMock()
    mock_file.file.file_size_in_bytes = file_size

    mock_scan = MagicMock()
    mock_scan.plan_files.return_value = [mock_file] * file_count

    mock_table = MagicMock()
    mock_table.name = MagicMock(return_value=name)
    mock_table.scan.return_value = mock_scan
    mock_table.location = location
    return mock_table
    


class MockLakeView:
    def __init__(self):
        self.tables = {
            "namespace1.table1": make_mock_table("table1", 200, 50_000, None),
            "namespace1.table2": make_mock_table("table2", 10, 1024**3, "some_location"),
            "namespace1.table3": make_mock_table("table3", 1_200_000, 49_000, "some_location"),
        }
        self.ns_tables = {
            "namespace1": ["namespace1.table1", "namespace1.table2", "namespace1.table3"],
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
    'namespace1.table2':["LARGE_FILES"], 
    'namespace1.table3':["SMALL_FILES", "SMALL_FILES_LARGE_TABLE"]
}


@pytest.mark.parametrize("table", [
        ('namespace1.table1'), 
        ('namespace1.table2'), 
        ('namespace1.table3')
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
    codes1 = {r.code for r in ns_results["namespace1.table1"]}
    codes2 = {r.code for r in ns_results["namespace1.table2"]}
    codes3 = {r.code for r in ns_results["namespace1.table3"]}
    assert set(codes1) == set(table_rules["namespace1.table1"])
    assert set(codes2) == set(table_rules["namespace1.table2"])
    assert set(codes3) == set(table_rules["namespace1.table3"])

def test_run_for_lakehouse():
    lakeview = MockLakeView()
    runner = InsightsRunner(lakeview)
    all_results = runner.run_for_lakehouse()
    assert "namespace1.table1" in all_results
    assert "namespace1.table2" in all_results
    assert "namespace1.table3" in all_results
    codes1 = [r.code for r in all_results["namespace1.table1"]]
    codes2 = [r.code for r in all_results["namespace1.table2"]]
    codes3 = [r.code for r in all_results["namespace1.table3"]]
    assert set(codes1) == set(table_rules["namespace1.table1"])
    assert set(codes2) == set(table_rules["namespace1.table2"])
    assert set(codes3) == set(table_rules["namespace1.table3"])


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
