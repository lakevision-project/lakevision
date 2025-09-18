from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Type, TypeVar, Generic
from dataclasses import dataclass

# A TypeVar is used to represent the specific dataclass type (e.g., User)
T = TypeVar("T")

class StorageInterface(Generic[T], ABC):
    """
    An abstract interface for storing and retrieving specific dataclass objects
    in a table structure.
    """
    def __init__(self, model: Type[T]):
        self.model = model
        self.table_name = model.__name__.lower() + 's'

    @abstractmethod
    def connect(self) -> None:
        """Establish the connection to the database."""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close the connection to the database."""
        pass

    @abstractmethod
    def ensure_table(self) -> None:
        """Ensure a table matching the dataclass schema exists."""
        pass

    @abstractmethod
    def save(self, item: T) -> None:
        """Save or replace a dataclass instance."""
        pass
    
    @abstractmethod
    def get_by_id(self, item_id: Any) -> Optional[T]:
        """Retrieve a dataclass instance by its ID."""
        pass

    @abstractmethod
    def get_all(self) -> List[T]:
        """Retrieve all dataclass instances from the table."""
        pass
    
    @abstractmethod
    def get_by_attribute(self, attribute: str, value: Any) -> List[T]:
        """Retrieve dataclass instances where a specific attribute matches a value."""
        pass

    @abstractmethod
    def delete(self, item_id: Any) -> None:
        """Delete an item by its ID."""
        pass