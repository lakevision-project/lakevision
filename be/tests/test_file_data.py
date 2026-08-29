"""Tests for the paged file listing.

`inspect.files()` carries full per-file column statistics, so a large table's
listing is hundreds of megabytes if returned whole -- one table in our test
catalog produces ~300 MB. These tests pin the curated column set, the paging
arithmetic, and the value formatting that makes the listing readable.
"""

import pandas as pd
import pytest


class _FakeInspect:
    def __init__(self, arrow_table):
        self._t = arrow_table

    def files(self):
        return self._t


class _FakeMetadata:
    def __init__(self, snapshot_id=1):
        self.current_snapshot_id = snapshot_id


class _FakeTable:
    def __init__(self, arrow_table, snapshot_id=1, name=("ns", "tbl")):
        self.inspect = _FakeInspect(arrow_table)
        self.metadata = _FakeMetadata(snapshot_id)
        self._name = name

    def name(self):
        return self._name


def _files_table(rows=3):
    import pyarrow as pa

    return pa.table(
        {
            "content": pa.array([0] * rows, pa.int8()),
            "file_path": pa.array([f"s3://bucket/wh/data/{i}.parquet" for i in range(rows)]),
            "file_format": pa.array(["PARQUET"] * rows),
            "spec_id": pa.array([0] * rows, pa.int32()),
            "partition": pa.array([{"d": f"2026-01-0{i % 9 + 1}"} for i in range(rows)]),
            "record_count": pa.array([100 + i for i in range(rows)], pa.int64()),
            "file_size_in_bytes": pa.array([1_000_000 + i for i in range(rows)], pa.int64()),
            "sort_order_id": pa.array([0] * rows, pa.int32()),
            # Present in real output and deliberately not returned.
            "lower_bounds": pa.array([None] * rows, pa.map_(pa.int32(), pa.binary())),
            "readable_metrics": pa.array([None] * rows, pa.struct([("x", pa.int64())])),
        }
    )


@pytest.fixture
def lakeview():
    from app.lakeviewer import LakeView

    lv = LakeView.__new__(LakeView)  # bypass env-driven __init__
    lv.catalog = None
    lv.namespace_options = []
    lv._files_cache = {}
    return lv


def test_returns_curated_columns_only(lakeview):
    """Binary bounds and nested metrics must not reach the client."""
    df, total = lakeview.get_file_data(_FakeTable(_files_table(3)), 0, 10)
    assert total == 3
    assert "lower_bounds" not in df.columns
    assert "readable_metrics" not in df.columns
    assert set(df.columns) >= {"Content", "File path", "Format", "Records", "Size (bytes)"}


def test_content_code_is_translated(lakeview):
    """content is an int8 code in the manifest; 0 means a data file."""
    df, _ = lakeview.get_file_data(_FakeTable(_files_table(1)), 0, 10)
    assert df["Content"].iloc[0] == "Data"


def test_partition_struct_is_flattened(lakeview):
    df, _ = lakeview.get_file_data(_FakeTable(_files_table(1)), 0, 10)
    assert df["Partition"].iloc[0] == "d=2026-01-01"


def test_paging_slices_and_reports_total(lakeview):
    table = _FakeTable(_files_table(25))
    first, total = lakeview.get_file_data(table, 0, 10)
    second, _ = lakeview.get_file_data(table, 10, 10)
    last, _ = lakeview.get_file_data(table, 20, 10)
    assert total == 25
    assert len(first) == 10 and len(second) == 10
    assert len(last) == 5, "final page should be short, not padded"
    assert first["File path"].iloc[0] != second["File path"].iloc[0]


def test_row_ids_are_absolute_across_pages(lakeview):
    """Ids must not restart per page, or keyed rendering collides between pages."""
    table = _FakeTable(_files_table(25))
    second, _ = lakeview.get_file_data(table, 10, 5)
    assert list(second["id"]) == [10, 11, 12, 13, 14]


def test_offset_past_end_returns_empty_not_error(lakeview):
    df, total = lakeview.get_file_data(_FakeTable(_files_table(3)), 500, 10)
    assert total == 3
    assert len(df) == 0


def test_limit_is_capped(lakeview):
    """An unbounded limit would defeat the point of paging."""
    df, _ = lakeview.get_file_data(_FakeTable(_files_table(2000)), 0, 100_000)
    assert len(df) <= 1000


def test_table_without_snapshot_returns_empty(lakeview):
    table = _FakeTable(_files_table(3), snapshot_id=None)
    assert lakeview.get_file_data(table, 0, 10) == ([], 0)


def test_manifests_are_read_once_per_snapshot(lakeview):
    """Paging must not re-read every manifest; that is the expensive part."""
    arrow = _files_table(30)
    table = _FakeTable(arrow)
    calls = []
    original = table.inspect.files

    def counting():
        calls.append(1)
        return original()

    table.inspect.files = counting

    lakeview.get_file_data(table, 0, 10)
    lakeview.get_file_data(table, 10, 10)
    lakeview.get_file_data(table, 20, 10)
    assert len(calls) == 1, "later pages should be served from the cache"


def test_cache_is_keyed_by_snapshot(lakeview):
    """A new snapshot must invalidate, or the listing goes stale after a commit."""
    table = _FakeTable(_files_table(5), snapshot_id=1)
    lakeview.get_file_data(table, 0, 5)
    table.metadata.current_snapshot_id = 2
    table.inspect = _FakeInspect(_files_table(9))
    _, total = lakeview.get_file_data(table, 0, 5)
    assert total == 9
