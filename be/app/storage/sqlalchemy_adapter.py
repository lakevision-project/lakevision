import dataclasses
import json
from typing import Any, Dict, List, Optional, Type
from sqlalchemy import (create_engine, text, inspect, Table, Column, MetaData,
                          String, Integer, Float, Boolean, Text)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

from .interface import StorageInterface, T

class SQLAlchemyStorage(StorageInterface[T]):
    """
    Stores a dataclass in a table with columns matching the dataclass fields.
    """
    def __init__(self, db_url: str, model: Type[T]):
        super().__init__(model)
        self._db_url = db_url
        self._engine: Optional[Engine] = None
        self._field_names = {f.name for f in dataclasses.fields(self.model)}
        self._complex_fields = {
            f.name for f in dataclasses.fields(self.model) if f.type in [list, dict, tuple]
        }
        print(f"✅ Initialized SQLAlchemyStorage for model '{model.__name__}'")

    def connect(self) -> None:
        if not self._engine:
            self._engine = create_engine(self._db_url)

    def disconnect(self) -> None:
        if self._engine:
            self._engine.dispose()
            self._engine = None

    def _get_engine(self) -> Engine:
        if not self._engine:
            raise ConnectionError("Database not connected.")
        return self._engine
    
    def _map_type(self, py_type: Type) -> Any:
        """Maps Python types to SQLAlchemy types."""
        if py_type is int: return Integer
        if py_type is float: return Float
        if py_type is bool: return Boolean
        # Use Text for strings and complex types that will be serialized to JSON
        if py_type in [str, list, dict, tuple]: return Text
        # Fallback for other types
        return Text

    def _serialize_row(self, row_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Serializes complex fields in a dictionary to JSON strings."""
        serialized = row_dict.copy()
        for field_name in self._complex_fields:
            if field_name in serialized and serialized[field_name] is not None:
                serialized[field_name] = json.dumps(serialized[field_name])
        return serialized

    def _deserialize_row(self, row_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Deserializes fields from JSON strings back into Python objects."""
        deserialized = row_dict.copy()
        for field_name in self._complex_fields:
            if field_name in deserialized and isinstance(deserialized[field_name], str):
                try:
                    deserialized[field_name] = json.loads(deserialized[field_name])
                except json.JSONDecodeError:
                    # Not a valid JSON string, leave as is
                    pass
        return deserialized

    def ensure_table(self) -> None:
        engine = self._get_engine()
        if not inspect(engine).has_table(self.table_name):
            metadata = MetaData()
            columns = []
            for field in dataclasses.fields(self.model):
                is_primary_key = field.name == 'id'
                # Use String(255) only for the primary key for better indexing
                sqlalchemy_type = String(255) if is_primary_key else self._map_type(field.type)
                columns.append(Column(field.name, sqlalchemy_type, primary_key=is_primary_key))
            Table(self.table_name, metadata, *columns)
            metadata.create_all(engine)
            print(f"Table '{self.table_name}' created with schema.")

    def save(self, item: T) -> None:
        engine = self._get_engine()
        item_dict = dataclasses.asdict(item)
        item_id = item_dict.get('id')
        
        serialized_dict = self._serialize_row(item_dict)

        with engine.begin() as conn:
            conn.execute(text(f"DELETE FROM {self.table_name} WHERE id = :id"), {"id": item_id})
            columns = ", ".join(serialized_dict.keys())
            placeholders = ", ".join(f":{key}" for key in serialized_dict.keys())
            stmt = text(f"INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})")
            conn.execute(stmt, serialized_dict)

    def get_by_id(self, item_id: Any) -> Optional[T]:
        engine = self._get_engine()
        with engine.connect() as conn:
            stmt = text(f"SELECT * FROM {self.table_name} WHERE id = :id")
            result = conn.execute(stmt, {"id": item_id}).mappings().first()
        
        if not result:
            return None
        
        deserialized_result = self._deserialize_row(dict(result))
        return self.model(**deserialized_result)

    def get_all(self) -> List[T]:
        engine = self._get_engine()
        with engine.connect() as conn:
            stmt = text(f"SELECT * FROM {self.table_name}")
            results = conn.execute(stmt).mappings().all()
        
        deserialized_results = [self._deserialize_row(dict(row)) for row in results]
        return [self.model(**row) for row in deserialized_results]

    def get_by_attribute(self, attribute: str, value: Any) -> List[T]:
        if attribute not in self._field_names:
            raise ValueError(f"'{attribute}' is not a valid field in {self.model.__name__}")
        
        engine = self._get_engine()
        
        # Serialize value if it's a complex type for querying
        query_value = json.dumps(value) if attribute in self._complex_fields else value
        
        with engine.connect() as conn:
            stmt = text(f"SELECT * FROM {self.table_name} WHERE {attribute} = :value")
            results = conn.execute(stmt, {"value": query_value}).mappings().all()
        
        deserialized_results = [self._deserialize_row(dict(row)) for row in results]
        return [self.model(**row) for row in deserialized_results]

    def delete(self, item_id: Any) -> None:
        engine = self._get_engine()
        with engine.begin() as conn:
            stmt = text(f"DELETE FROM {self.table_name} WHERE id = :id")
            conn.execute(stmt, {"id": item_id})