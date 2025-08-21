from unittest.mock import MagicMock
from app.insights.rules import rule_small_files, rule_no_location, Insight
from app.insights.runner import InsightsRunner

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
        }
        self.ns_tables = {
            "namespace1": ["namespace1.table1", "namespace1.table2"],
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

def test_run_for_table():
    lakeview = MockLakeView()
    runner = InsightsRunner(lakeview)
    results = runner.run_for_table("namespace1.table1")
    codes = {r.code for r in results}
    assert "SMALL_FILES" in codes
    assert "NO_LOCATION" in codes
    assert "LARGE_FILES" not in codes

def test_run_for_namespace():
    lakeview = MockLakeView()
    runner = InsightsRunner(lakeview)
    ns_results = runner.run_for_namespace("namespace1")
    assert "namespace1.table1" in ns_results
    assert "namespace1.table2" in ns_results
    codes1 = {r.code for r in ns_results["namespace1.table1"]}
    codes2 = {r.code for r in ns_results["namespace1.table2"]}
    assert "SMALL_FILES" in codes1  # Should trigger for table1 only
    assert "NO_LOCATION" in codes1
    assert "LARGE_FILES" not in codes1
    assert "SMALL_FILES" not in codes2  # Not enough files
    assert "NO_LOCATION" not in codes2  # Has a location
    assert "LARGE_FILES" in codes2

def test_run_for_lakehouse():
    lakeview = MockLakeView()
    runner = InsightsRunner(lakeview)
    all_results = runner.run_for_lakehouse()
    assert "namespace1.table1" in all_results
    assert "namespace1.table2" in all_results
    codes = [r.code for r in all_results["namespace1.table1"]]
    assert "SMALL_FILES" in codes
    assert "NO_LOCATION" in codes
    assert "NO_LOCATION" in codes