def qualified_table_name(identifier) -> str:
    """Returns fully qualified table name as a string, dot-separated."""
    # Defensive: If identifier is a string, just return it.
    if isinstance(identifier, str):
        return identifier
    return ".".join(str(part) for part in identifier)