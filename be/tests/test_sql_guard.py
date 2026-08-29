"""Tests for server-side SQL validation.

These guard an authorization boundary, not a style rule: Daft's SQL dialect can
read arbitrary paths via read_parquet/read_csv, so an unvalidated `sql` parameter
is arbitrary file read as the backend service account. The exploit cases below
were all confirmed to work against the unpatched code.
"""

import pytest

from app.sql_guard import SQLValidationError, validate_and_bind, validate_sql

TABLE = "sales.orders"


# --- Queries that must keep working -----------------------------------------

@pytest.mark.parametrize(
    "sql",
    [
        "SELECT id, region WHERE id > 1",          # UI shape: FROM omitted
        "SELECT *",
        "SELECT * FROM sales.orders",              # fully qualified
        "SELECT * FROM orders",                    # bare table name
        "SELECT * FROM df LIMIT 5",                # pre-bound alias
        "SELECT region, sum(amount) AS s FROM sales.orders GROUP BY region",
        "SELECT count(*) FROM sales.orders",
        "SELECT * FROM sales.orders ORDER BY amount DESC LIMIT 10",
        "SELECT * FROM sales.orders WHERE region IN ('us', 'eu')",
    ],
)
def test_legitimate_queries_are_allowed(sql):
    assert validate_sql(sql, TABLE)


def test_table_reference_is_rewritten_to_df_alias():
    """get_sample_data registers the authorized table with Daft as `df`."""
    assert validate_and_bind("SELECT id FROM sales.orders", TABLE) == "SELECT id FROM df"
    assert validate_and_bind("SELECT id WHERE id > 1", TABLE) == "SELECT id FROM df WHERE id > 1"


def test_rewrite_does_not_corrupt_string_literals():
    """Regression: the previous str.replace() also rewrote matching literals."""
    out = validate_and_bind(
        "SELECT * FROM sales.orders WHERE region = 'sales.orders'", TABLE
    )
    assert "'sales.orders'" in out
    assert out.count("df") == 1


# --- Exploits that must be blocked ------------------------------------------

@pytest.mark.parametrize(
    "sql,reason",
    [
        ("SELECT * FROM read_parquet('/warehouse/other/data/f.parquet')", "file read"),
        ("SELECT * FROM read_csv('/etc/passwd')", "local file read"),
        ("SELECT * FROM read_parquet('/warehouse/**/*.parquet')", "glob read"),
        ("SELECT * FROM read_json('/tmp/x.json')", "json file read"),
        ("SELECT * FROM read_iceberg('/tmp/tbl')", "direct iceberg path"),
        ("SELECT * FROM sales.private_keys", "different table"),
        ("SELECT * FROM other_ns.customers", "different namespace"),
        ("SELECT * FROM sales.orders JOIN sales.private_keys USING (id)", "join"),
        ("SELECT * FROM df UNION SELECT * FROM sales.private_keys", "union"),
        ("SELECT * FROM df WHERE id IN (SELECT k FROM sales.private_keys)", "subquery"),
        ("WITH x AS (SELECT * FROM sales.private_keys) SELECT * FROM x", "cte"),
        ("SELECT 1; SELECT * FROM sales.private_keys", "stacked statements"),
        ("DELETE FROM sales.orders", "non-select"),
        ("INSERT INTO sales.orders VALUES (1, 'x', 2.0)", "insert"),
        ("UPDATE sales.orders SET region = 'x'", "update"),
        ("DROP TABLE sales.orders", "ddl"),
        ("SELECT 1 AS x", "reads no column of the table"),
        ("", "empty"),
        ("   ", "whitespace only"),
    ],
)
def test_exploits_and_malformed_input_are_rejected(sql, reason):
    with pytest.raises(SQLValidationError):
        validate_and_bind(sql, TABLE)


def test_rejection_message_names_the_authorized_table():
    with pytest.raises(SQLValidationError) as exc:
        validate_sql("SELECT * FROM sales.private_keys", TABLE)
    assert "sales.orders" in str(exc.value)


def test_nested_namespace_table_is_accepted():
    tid = "lake.raw.events"
    assert validate_and_bind("SELECT id FROM lake.raw.events", tid) == "SELECT id FROM df"
    assert validate_and_bind("SELECT id FROM events", tid) == "SELECT id FROM df"


def test_default_catalog_prefix_is_tolerated():
    """get_sample_data strips a leading 'default.' before validating."""
    assert validate_and_bind("SELECT id FROM sales.orders", "default.sales.orders")


def test_unparseable_sql_is_rejected_not_raised_as_internal_error():
    with pytest.raises(SQLValidationError):
        validate_and_bind("SELECT FROM WHERE ((((", TABLE)


def test_file_reading_functions_blocked_regardless_of_sqlglot_node_shape():
    """sqlglot promotes read_* to dedicated node classes at different versions.

    In 26.x ``read_parquet`` parses to a generic Anonymous node whose .name is
    "read_parquet"; in 30.x it is a ReadParquet node whose .name is empty. A
    name-only check silently stopped matching across that upgrade, so the guard
    matches on node type as well.
    """
    import sqlglot
    from sqlglot import exp

    from app.sql_guard import _FILE_READING_FUNCTIONS, _FILE_READING_NODE_TYPES

    for func_name in sorted(_FILE_READING_FUNCTIONS):
        sql = f"SELECT * FROM {func_name}('/tmp/whatever')"
        with pytest.raises(SQLValidationError):
            validate_and_bind(sql, TABLE)

        # Whichever way this version parses it, one of the two checks must fire.
        parsed = sqlglot.parse_one(sql, read="duckdb")
        funcs = tuple(parsed.find_all(exp.Func))
        if funcs:
            matched = any(
                (getattr(f, "name", "") or "").lower() in _FILE_READING_FUNCTIONS
                or type(f).__name__.lower() in _FILE_READING_NODE_TYPES
                for f in funcs
            )
            assert matched, f"{func_name} parsed to an unrecognised node shape"


def test_rejection_message_does_not_echo_the_supplied_path():
    """The path is attacker-controlled; keep it out of the response body."""
    with pytest.raises(SQLValidationError) as exc:
        validate_and_bind("SELECT * FROM read_csv('/etc/passwd')", TABLE)
    assert "/etc/passwd" not in str(exc.value)
