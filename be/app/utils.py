import os

# Helper function to parse 'true', 'True', '1' as boolean True
def get_bool_env(var_name: str) -> bool:
    """Gets a boolean value from an environment variable."""
    return os.getenv(var_name, 'false').lower() in ('true', '1')