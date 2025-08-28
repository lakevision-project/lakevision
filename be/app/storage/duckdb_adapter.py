import dataclasses
import json
from typing import Any, List, Optional, Type
import duckdb

from .interface import StorageInterface, T

class DuckDBStorage(StorageInterface[T]):
    """
    A unified, model-aware adapter for DuckDB. Supports native and attached modes.
    """
    ATTACH_CONFIG = {
        "postgresql": {"extension": "postgres", "type": "POSTGRES"},
        "mysql": {"extension": "mysql_scanner", "type": "MYSQL"},
        "sqlite": {"extension": "sqlite_scanner", "type": "SQLITE"},
    }

    def __init__(self, db_url: str, model: Type[T]):
        super().__init__(model)
        self._db_url = db_url
        self._con: Optional[duckdb.DuckDBPyConnection] = None
        self._mode = 'native'
        self._db_alias = "attached_db"
        self.attached_db_type = ""
        self._field_names = {f.name for f in dataclasses.fields(self.model)}
        self._complex_fields = {
            f.name for f in dataclasses.fields(self.model) if f.type in [list, dict, tuple]
        }

        scheme = self._db_url.split("://")[0]
        if scheme in self.ATTACH_CONFIG:
            self._mode = 'attached'
        print(f"✅ Initialized DuckDBStorage for model '{model.__name__}' in '{self._mode}' mode.")

    def connect(self) -> None:
        if self._con: return

        if self._mode == 'attached':
            self._con = duckdb.connect(database=':memory:')
            scheme = self._db_url.split("://")[0]
            config = self.ATTACH_CONFIG[scheme]
            self.attached_db_type = config["type"]
            attach_path = self._db_url.replace('sqlite:///', '') if scheme == "sqlite" else self._db_url
            try:
                self._con.execute(f"INSTALL {config['extension']};")
                self._con.execute(f"LOAD {config['extension']};")
                self._con.execute(f"ATTACH '{attach_path}' AS {self._db_alias} (TYPE {self.attached_db_type});")
            except Exception as e:
                raise ConnectionError(f"Failed to attach {self.attached_db_type} database: {e}")
        else:
            db_path = self._db_url.replace('duckdb:///', '')
            self._con = duckdb.connect(database=db_path, read_only=False)

    def disconnect(self) -> None:
        if self._con: self._con.close()

    def _get_con(self) -> duckdb.DuckDBPyConnection:
        if not self._con: raise ConnectionError("Database not connected.")
        return self._con
    
    def _get_full_table_name(self) -> str:
        return f"{self._db_alias}.{self.table_name}" if self._mode == 'attached' else self.table_name

    def _map_type(self, py_type: Type) -> str:
        if py_type is int: return "INTEGER"
        if py_type is float: return "DOUBLE"
        if py_type is bool: return "BOOLEAN"
        if py_type in [str, list, dict, tuple]: return "VARCHAR"
        return "VARCHAR"

    def _deserialize_row(self, row_dict: dict[str, Any]) -> dict[str, Any]:
        """Deserializes fields from JSON strings back into Python objects."""
        deserialized = row_dict.copy()
        for field_name in self._complex_fields:
            if field_name in deserialized and isinstance(deserialized[field_name], str):
                try:
                    deserialized[field_name] = json.loads(deserialized[field_name])
                except json.JSONDecodeError:
                    pass
        return deserialized

    def _fetchall_dicts(self, cursor) -> List[dict[str, Any]]:
        """Helper to convert a DuckDB cursor result into a list of dictionaries."""
        column_names = [desc[0] for desc in cursor.description]
        rows = [dict(zip(column_names, row)) for row in cursor.fetchall()]
        return [self._deserialize_row(row) for row in rows]

    def ensure_table(self) -> None:
        con = self._get_con()
        full_table_name = self._get_full_table_name()
        column_defs = []
        for field in dataclasses.fields(self.model):
            pk_str = "PRIMARY KEY" if field.name == 'id' else ""
            sql_type = self._map_type(field.type)
            column_defs.append(f"{field.name} {sql_type} {pk_str}")
        create_sql = f"CREATE TABLE IF NOT EXISTS {full_table_name} ({', '.join(column_defs)});"
        con.execute(create_sql)

    def save(self, item: T) -> None:
        con = self._get_con()
        full_table_name = self._get_full_table_name()
        item_dict = dataclasses.asdict(item)
        
        # Serialize complex types
        for field_name in self._complex_fields:
            if field_name in item_dict and item_dict[field_name] is not None:
                item_dict[field_name] = json.dumps(item_dict[field_name])
        
        columns = ", ".join(item_dict.keys())
        placeholders = ", ".join("?" for _ in item_dict)
        values = list(item_dict.values())
        
        if self._mode == 'attached':
            try:
                con.execute("BEGIN TRANSACTION;")
                con.execute(f"DELETE FROM {full_table_name} WHERE id = ?", [item_dict['id']])
                con.execute(f"INSERT INTO {full_table_name} ({columns}) VALUES ({placeholders})", values)
                con.execute("COMMIT;")
            except Exception as e:
                con.execute("ROLLBACK;")
                raise IOError(f"Failed to save item: {e}")
        else:
            con.execute(f"INSERT OR REPLACE INTO {full_table_name} ({columns}) VALUES ({placeholders});", values)

    def get_by_id(self, item_id: Any) -> Optional[T]:
        con = self._get_con()
        stmt = f"SELECT * FROM {self._get_full_table_name()} WHERE id = ?"
        cursor = con.execute(stmt, [item_id])
        results = self._fetchall_dicts(cursor)
        return self.model(**results[0]) if results else None

    def get_all(self) -> List[T]:
        con = self._get_con()
        stmt = f"SELECT * FROM {self._get_full_table_name()}"
        cursor = con.execute(stmt)
        results = self._fetchall_dicts(cursor)
        return [self.model(**row) for row in results]

    def get_by_attribute(self, attribute: str, value: Any) -> List[T]:
        if attribute not in self._field_names:
            raise ValueError(f"'{attribute}' is not a valid field in {self.model.__name__}")
        
        query_value = json.dumps(value) if attribute in self._complex_fields else value
        
        con = self._get_con()
        stmt = f"SELECT * FROM {self._get_full_table_name()} WHERE {attribute} = ?"
        cursor = con.execute(stmt, [query_value])
        results = self._fetchall_dicts(cursor)
        return [self.model(**row) for row in results]

    def delete(self, item_id: Any) -> None:
        con = self._get_con()
        stmt = f"DELETE FROM {self._get_full_table_name()} WHERE id = ?"
        con.execute(stmt, [item_id])