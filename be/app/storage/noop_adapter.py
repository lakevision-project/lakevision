from typing import Optional, Type, List, Any

from app.storage.interface import StorageInterface, T, AggregateFunction

# A dummy storage interface that does nothing, to be used when health is disabled.
# It implements the interface so the rest of the app doesn't crash.
class NoOpStorage(StorageInterface[T]):
    def __init__(self, model: Type[T]):
        self.model = model
        print(f"Health feature disabled. Initializing NoOpStorage for {model.__name__}.")

    def connect(self) -> None:
        pass  # Do nothing
    
    def disconnect(self) -> None:
        pass  # Do nothing

    def ensure_table(self) -> None:
        pass  # Do nothing

    def save(self, item: T) -> None:
        raise NotImplementedError("Health feature is disabled. Cannot create data.")

    def get_by_id(self, item_id: Any) -> Optional[T]:
        return None
    
    def get_all(self, id: str) -> Optional[T]:
        return None

    def delete(self, id: str) -> bool:
        return False
  
    def get_aggregate(
        self,
        func: AggregateFunction,
        column: str,
        criteria: dict[str, Any] | None = None,
        group_by: List[str] | None = None
    ) -> Any:
        return None

    def get_by_attributes(
        self,
        criteria: dict[str, Any],
        skip: Optional[int] = None,
        limit: Optional[int] = None
    ) -> List[T]:
        return None
    
    def get_by_attribute(
        self,
        attribute: str,
        value: Any,
        skip: Optional[int] = None,
        limit: Optional[int] = None
    ) -> List[T]:
        return None
    
    def execute_raw_select_query(self, sql_query: str, params: dict[str, Any] | None = None) -> List[dict[str, Any]]:
        return False

    def find_by_raw_query(self, sql_query: str, params: dict[str, Any] | None = None) -> List[T]:
        return False