class LVException(Exception):
    """Custom exception for LakeView application errors."""
    def __init__(self, name: str, message: str):
        self.name = name
        self.message = message