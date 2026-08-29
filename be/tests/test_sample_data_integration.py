"""End-to-end checks of the sample-data query path against a real Iceberg catalog.

Builds a small local SQLite-backed catalog (no network, no object storage) with
two tables: one the query is authorized for, and one standing in for data the
caller must not reach. The exploits below all succeeded before validation was
added -- `read_csv` in particular returned the contents of an arbitrary local
file, i.e. arbitrary file read as the backend service account.
"""

import os
import shutil
import tempfile

import pytest

pytest.importorskip("daft")


def _first_data_file(table) -> str:
    """Path of a table's first data file, read from Iceberg metadata.

    Derived from the manifest rather than a hardcoded warehouse path: the
    on-disk layout is an implementation detail that changes between pyiceberg
    releases (0.11 dropped the Hive-style "<namespace>.db" directory).
    """
    for task in table.scan().plan_files():
        path = task.file.file_path
        return path[len("file://"):] if path.startswith("file://") else path
    raise AssertionError("fixture table has no data files")


@pytest.fixture(scope="module")
def fixture_catalog():
    tmp = tempfile.mkdtemp(prefix="lv_sqlguard_")
    warehouse = os.path.join(tmp, "wh")
    os.makedirs(warehouse, exist_ok=True)

    from pyiceberg import catalog as pyi_catalog
    from pyiceberg.io import FSSPEC_FILE_IO, PY_IO_IMPL
    import pyarrow as pa

    cat = pyi_catalog.load_catalog(
        "default",
        **{
            "type": "sql",
            "uri": f"sqlite:///{os.path.join(tmp, 'cat.db')}",
            "warehouse": f"file://{warehouse}",
            PY_IO_IMPL: FSSPEC_FILE_IO,
        },
    )
    cat.create_namespace("sales")

    orders = pa.table({
        "id": pa.array([1, 2, 3], pa.int64()),
        "region": pa.array(["us", "eu", "apac"]),
        "amount": pa.array([10.5, 20.0, 30.25], pa.float64()),
    })
    cat.create_table("sales.orders", schema=orders.schema).append(orders)

    secrets_tbl = pa.table({
        "secret_id": pa.array([9], pa.int64()),
        "token": pa.array(["CONFIDENTIAL"]),
    })
    cat.create_table("sales.private_keys", schema=secrets_tbl.schema).append(secrets_tbl)

    secret_file = os.path.join(tmp, "local_secret.csv")
    with open(secret_file, "w") as handle:
        handle.write("k,v\nrootpw,hunter2\n")

    yield {
        "catalog": cat,
        "root": tmp,
        "warehouse": warehouse,
        "secret_file": secret_file,
        "victim_file": _first_data_file(cat.load_table("sales.private_keys")),
    }
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.fixture(scope="module")
def lakeview(fixture_catalog):
    from app.lakeviewer import LakeView

    lv = LakeView.__new__(LakeView)          # bypass env-driven __init__
    lv.catalog = fixture_catalog["catalog"]
    lv.namespace_options = []
    return lv


@pytest.fixture(scope="module")
def orders(lakeview):
    return lakeview.load_table("sales.orders")


def test_unfiltered_sample_still_returns_rows(lakeview, orders):
    df = lakeview.get_sample_data(orders, None, 10)
    assert len(df) == 3


def test_legitimate_query_returns_expected_rows(lakeview, orders):
    df = lakeview.get_sample_data(orders, "SELECT id, region WHERE id > 1", 10)
    assert sorted(df["region"].tolist()) == ["apac", "eu"]
    assert "amount" not in df.columns


def test_legitimate_aggregate_query_runs(lakeview, orders):
    df = lakeview.get_sample_data(
        orders, "SELECT region, sum(amount) AS s FROM sales.orders GROUP BY region", 10
    )
    assert dict(zip(df["region"], df["s"]))["apac"] == pytest.approx(30.25)


def test_read_parquet_cannot_reach_another_tables_files(lakeview, orders, fixture_catalog):
    from app.sql_guard import SQLValidationError

    victim = fixture_catalog["victim_file"]
    assert os.path.exists(victim), "fixture should have written a data file"
    with pytest.raises(SQLValidationError):
        lakeview.get_sample_data(orders, f"SELECT * FROM read_parquet('{victim}')", 10)


def test_read_csv_cannot_read_arbitrary_local_files(lakeview, orders, fixture_catalog):
    """This returned {'k': 'rootpw', 'v': 'hunter2'} before the fix."""
    from app.sql_guard import SQLValidationError

    with pytest.raises(SQLValidationError):
        lakeview.get_sample_data(
            orders, f"SELECT * FROM read_csv('{fixture_catalog['secret_file']}')", 10
        )


def test_glob_read_is_blocked(lakeview, orders, fixture_catalog):
    from app.sql_guard import SQLValidationError

    pattern = os.path.join(fixture_catalog["warehouse"], "**", "*.parquet")
    with pytest.raises(SQLValidationError):
        lakeview.get_sample_data(orders, f"SELECT * FROM read_parquet('{pattern}')", 10)


def test_other_catalog_table_is_blocked(lakeview, orders):
    from app.sql_guard import SQLValidationError

    with pytest.raises(SQLValidationError):
        lakeview.get_sample_data(orders, "SELECT * FROM sales.private_keys", 10)


def test_confidential_value_never_appears_in_any_result(lakeview, orders, fixture_catalog):
    """Belt-and-braces: no accepted query yields the other table's secret."""
    from app.sql_guard import SQLValidationError

    victim = fixture_catalog["victim_file"]
    attempts = [
        "SELECT * FROM sales.private_keys",
        f"SELECT * FROM read_parquet('{victim}')",
        "SELECT * FROM df UNION SELECT secret_id, token, token FROM sales.private_keys",
        "SELECT * FROM sales.orders JOIN sales.private_keys USING (id)",
    ]
    for sql in attempts:
        try:
            df = lakeview.get_sample_data(orders, sql, 10)
        except SQLValidationError:
            continue
        assert "CONFIDENTIAL" not in df.to_csv(index=False), f"leaked via: {sql}"
