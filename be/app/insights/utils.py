from typing import Tuple

def qualified_table_name(identifier) -> str:
    """Returns fully qualified table name as a string, dot-separated."""
    # Defensive: If identifier is a string, just return it.
    if isinstance(identifier, str):
        return identifier
    return ".".join(str(part) for part in identifier)

def get_namespace_and_table_name(iceberg_table_name: str) -> Tuple[str, str]:
    parts = iceberg_table_name.split(".")
    num_parts = len(parts)
    if num_parts >= 2:
        return '.'.join(parts[:num_parts -1]), parts[-1]
    else:
        raise ValueError(f"Unexpected table structure: {iceberg_table_name}")