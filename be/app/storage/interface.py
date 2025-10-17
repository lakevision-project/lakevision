from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Type, TypeVar, Generic, Literal
from dataclasses import dataclass

# A TypeVar is used to represent the specific dataclass type (e.g., User)
T = TypeVar("T")
AggregateFunction = Literal["MIN", "MAX", "AVG", "SUM", "COUNT"]

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
    def get_aggregate(
        self,
        func: AggregateFunction,
        column: str,
        criteria: dict[str, Any] | None = None,
        group_by: List[str] | None = None
    ) -> Any:
        """
        Calculates an aggregate value (MIN, MAX, COUNT, etc.) for a column.
        """
        pass

    @abstractmethod
    def get_by_attributes(
        self,
        criteria: dict[str, Any],
        skip: Optional[int] = None,
        limit: Optional[int] = None
    ) -> List[T]:
        """Retrieve dataclass instances that match all specified criteria.."""
        pass
    
    @abstractmethod
    def get_by_attribute(
        self,
        attribute: str,
        value: Any,
        skip: Optional[int] = None,
        limit: Optional[int] = None
    ) -> List[T]:
        """Retrieve dataclass instances where a specific attribute matches a value."""
        pass

    @abstractmethod
    def delete(self, item_id: Any) -> None:
        """Delete an item by its ID."""
        pass

    @abstractmethod
    def execute_raw_select_query(self, sql_query: str, params: dict[str, Any] | None = None) -> List[dict[str, Any]]:
        """Executes a raw, parameterized SELECT query and returns the results."""
        pass

    @abstractmethod
    def find_by_raw_query(self, sql_query: str, params: dict[str, Any] | None = None) -> List[T]:
        """
        Executes a raw SELECT query and returns the results as a list of model instances.
        """
        pass