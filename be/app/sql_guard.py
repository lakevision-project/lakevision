"""Server-side validation for user-supplied SQL in the sample-data endpoint.

The frontend also validates queries, but that check is advisory only: the API is
reachable directly, so every restriction that matters is enforced here.

The threat model is specific. ``LakeView.get_sample_data`` hands the query to
``daft.sql()`` with a single DataFrame registered as ``df``. Daft's SQL dialect
exposes table-valued functions such as ``read_parquet``/``read_csv`` that take a
path and bypass the Iceberg catalog entirely, which means an unvalidated query is
arbitrary file read as the backend service account. Restricting the query to the
one authorized table is therefore an authorization boundary, not just hygiene.

The validator is deliberately an allowlist: anything it does not positively
recognise is rejected.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp

# Table-valued functions that take a path and read it directly, bypassing the
# catalog. Rejected outright rather than path-checked: there is no legitimate
# reason for a sample-data query to name a file.
_FILE_READING_FUNCTIONS = frozenset(
    {
        "read_parquet",
        "read_csv",
        "read_json",
        "read_deltalake",
        "read_iceberg",
        "read_hudi",
        "read_lance",
        "read_sql",
        "read_warc",
        "read_video_frames",
    }
)

# The same set as sqlglot node class names (e.g. ReadParquet -> "readparquet").
# Kept in sync with _FILE_READING_FUNCTIONS by derivation, not by hand.
_FILE_READING_NODE_TYPES = frozenset(
    name.replace("_", "") for name in _FILE_READING_FUNCTIONS
)

# Expression types that let a query reach beyond a plain single-table SELECT.
_FORBIDDEN_NODES = (
    (exp.Into, "SELECT ... INTO"),
    (exp.Join, "JOIN"),
    (exp.Union, "UNION"),
    (exp.Except, "EXCEPT"),
    (exp.Intersect, "INTERSECT"),
    (exp.With, "common table expression (WITH)"),
    (exp.Subquery, "subquery"),
)


class SQLValidationError(ValueError):
    """Raised when a user-supplied query fails validation."""


def _normalize_identifier(name: str) -> str:
    return name.strip().strip('"').strip("`").lower()


def _allowed_table_forms(table_id: str) -> set[str]:
    """Return the accepted spellings of the single authorized table.

    ``table_id`` arrives as ``namespace.table`` (the namespace may itself be
    dotted for nested namespaces). A query may name the table fully qualified or
    by its bare final component, and ``get_sample_data`` additionally rewrites
    the reference to ``df`` before handing the query to Daft.
    """
    parts = [p for p in table_id.split(".") if p]
    if not parts:
        raise SQLValidationError("No table context for this query.")
    forms = {_normalize_identifier(table_id), _normalize_identifier(parts[-1]), "df"}
    # A leading "default." catalog prefix is stripped elsewhere in the app.
    if len(parts) > 1 and parts[0].lower() == "default":
        forms.add(_normalize_identifier(".".join(parts[1:])))
    return forms


def validate_sql(sql: str, table_id: str) -> str:
    """Validate a user query, returning it normalized.

    Enforces: exactly one statement; a SELECT at the root; no file-reading
    functions; no joins/unions/CTEs/subqueries; and every table reference
    resolving to ``table_id``.

    Raises ``SQLValidationError`` with a message safe to show a user.
    """
    if not sql or not sql.strip():
        raise SQLValidationError("Query is empty.")

    try:
        statements = sqlglot.parse(sql, read="duckdb")
    except sqlglot.errors.ParseError as exc:
        raise SQLValidationError(f"Could not parse SQL: {exc}") from exc

    statements = [s for s in statements if s is not None]
    if len(statements) != 1:
        raise SQLValidationError("Exactly one SQL statement is allowed.")

    statement = statements[0]
    if not isinstance(statement, exp.Select):
        raise SQLValidationError("Only SELECT queries are allowed.")

    for node_type, label in _FORBIDDEN_NODES:
        if tuple(statement.find_all(node_type)):
            raise SQLValidationError(f"{label} is not allowed.")

    # Reject file-reading table-valued functions. Matched on both the parsed
    # node type and the function name: sqlglot promotes these to dedicated
    # classes at different versions (26 parses read_parquet as a generic
    # Anonymous node, 30 as ReadParquet with an empty .name), so neither check
    # alone is stable across releases.
    for func in statement.find_all(exp.Func):
        name = (getattr(func, "name", "") or "").lower()
        type_name = type(func).__name__.lower()
        if name in _FILE_READING_FUNCTIONS or type_name in _FILE_READING_NODE_TYPES:
            # Report the function, never `name` -- for these nodes `.name` can be
            # the file path the caller supplied, which we should not echo back.
            label = name if name in _FILE_READING_FUNCTIONS else type(func).__name__
            raise SQLValidationError(
                f"Reading files directly is not allowed ({label}); "
                "queries may only read the selected table."
            )

    allowed = _allowed_table_forms(table_id)
    referenced = tuple(statement.find_all(exp.Table))
    if not referenced:
        # The UI asks users to omit FROM and it is injected below. A query with
        # no table reference at all is therefore the expected input shape, not
        # an error -- but it must gain a FROM before it reaches Daft, otherwise
        # "SELECT 1" style probing would run unscoped.
        if not any(
            isinstance(e, (exp.Column, exp.Star)) or tuple(e.find_all(exp.Column, exp.Star))
            for e in statement.expressions
        ):
            # Nothing in the projection refers to a column, so the query does not
            # actually read the table (e.g. "SELECT 1"). Reject rather than
            # scope it, to keep the endpoint's contract narrow.
            raise SQLValidationError("Query must select at least one column from the current table.")
        statement = statement.from_(exp.to_table(table_id))
        referenced = tuple(statement.find_all(exp.Table))

    for table in referenced:
        parts = [
            _normalize_identifier(p.name)
            for p in (table.args.get("catalog"), table.args.get("db"), table.this)
            if p is not None and getattr(p, "name", None)
        ]
        if not parts:
            raise SQLValidationError("Could not determine the table being queried.")
        full = ".".join(parts)
        if full not in allowed and parts[-1] not in allowed:
            raise SQLValidationError(
                f"Query may only read '{table_id}', not '{full}'."
            )

    return statement.sql(dialect="duckdb")


def validate_and_bind(sql: str, table_id: str) -> str:
    """Validate ``sql`` and rewrite its table reference to the ``df`` alias.

    ``LakeView.get_sample_data`` registers the authorized Iceberg table with Daft
    under the name ``df``. Rewriting through the AST replaces an earlier
    ``str.replace`` of the table name, which could also corrupt a matching
    substring inside a string literal.
    """
    statement = sqlglot.parse_one(validate_sql(sql, table_id), read="duckdb")
    for table in statement.find_all(exp.Table):
        table.set("catalog", None)
        table.set("db", None)
        table.set("this", exp.to_identifier("df"))
    return statement.sql(dialect="duckdb")
