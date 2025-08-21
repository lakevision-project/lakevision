from pyiceberg.table import Table
from pyiceberg.types import StructType, ListType, MapType, UUIDType
from typing import Optional
from dataclasses import dataclass
from app.insights.utils import qualified_table_name
import yaml
import os
import pyarrow.compute as pc

rules_yaml_path = os.path.join(os.path.dirname(__file__), "rules.yaml")

SEVERAL_FILES = 100
AVERAGE_SMALL_FILES_IN_BYTES = 100_000
ONE_GB_IN_BYTES = 1000**3  # 1 GB in bytes (1024**3 is the actual value)
LARGE_TABLE_IN_BYTES= 50 * ONE_GB_IN_BYTES
AVERAGE_SMALL_FILES_LARGE_TABLES_IN_BYTES = 50_000
MAX_SNAPSHOTS_RECOMMENDED = 500

# Load yaml at app startup
with open(rules_yaml_path) as f:
    INSIGHT_META = yaml.safe_load(f)

@dataclass
class Insight:
    code: str
    table: str
    message: str
    severity: str
    suggested_action: str

def rule_small_files(table: Table) -> Optional[Insight]:
    files = [file_scan_task.file.file_size_in_bytes for file_scan_task in table.scan().plan_files()]
    if not files:
        return None
    len_files = len(files)
    avg_size = sum(files) / len_files
    if len_files > SEVERAL_FILES and avg_size < AVERAGE_SMALL_FILES_IN_BYTES:
        meta = INSIGHT_META["SMALL_FILES"]
        return Insight(
            code="SMALL_FILES",
            table=qualified_table_name(table.name()),
            message=meta["message"].format(num_files=len(files), avg_size=int(avg_size)),
            severity=meta["severity"],
            suggested_action=meta["suggested_action"]
        )
    return None

def rule_no_location(table: Table) -> Optional[Insight]:
    if not getattr(table, "location", None):
        meta = INSIGHT_META["NO_LOCATION"]
        return Insight(
            code = "NO_LOCATION",
            table=qualified_table_name(table.name()),
            message=meta["message"],
            severity=meta["severity"],
            suggested_action=meta["suggested_action"]
        )
    return None

def rule_large_files(table: Table) -> Optional[Insight]:
    files = [file_scan_task.file.file_size_in_bytes for file_scan_task in table.scan().plan_files()]
    if not files:
        return None
    avg_size = sum(files) / len(files)
    large_files = [file_size for file_size in files if file_size >= ONE_GB_IN_BYTES]
    num_large_files = len(large_files)
    if num_large_files >= 1:
        meta = INSIGHT_META["LARGE_FILES"]
        return Insight(
            code="LARGE_FILES",
            table=qualified_table_name(table.name()),
            message=meta["message"].format(num_files=len(files), avg_size=int(avg_size), num_large_files=num_large_files, max_size=ONE_GB_IN_BYTES),
            severity=meta["severity"],
            suggested_action=meta["suggested_action"]
        )
    return None

def rule_small_files_large_table(table: Table) -> Optional[Insight]:
    files = [file_scan_task.file.file_size_in_bytes for file_scan_task in table.scan().plan_files()]
    if not files:
        return None
    total_size = sum(files)
    avg_size = total_size / len(files)

    if avg_size < AVERAGE_SMALL_FILES_LARGE_TABLES_IN_BYTES and total_size > LARGE_TABLE_IN_BYTES:
        meta = INSIGHT_META["SMALL_FILES_LARGE_TABLE"]
        return Insight(
            code="SMALL_FILES_LARGE_TABLE",
            table=qualified_table_name(table.name()),
            message=meta["message"].format(avg_size=int(avg_size), total_size=int(total_size)),
            severity=meta["severity"],
            suggested_action=meta["suggested_action"]
        )
    return None

def search_for_uuid_column(schema) -> bool:
    for field in schema.fields:
        if isinstance(field.field_type, UUIDType):
            return True
        # This part handles the nested types
        # Recurse into nested struct types
        if isinstance(field.field_type, StructType):
            if search_for_uuid_column(field.field_type):
                return True        
        # Recurse into nested list types
        elif isinstance(field.field_type, ListType):
            element_type = field.field_type.element_type
            if isinstance(element_type, UUIDType):
                return True
            if isinstance(element_type, StructType):
                if search_for_uuid_column(element_type):
                    return True                
        # Recurse into nested map types
        elif isinstance(field.field_type, MapType):
            key_type = field.field_type.key_type
            value_type = field.field_type.value_type
            if isinstance(key_type, UUIDType):
                return True
            if isinstance(value_type, UUIDType):
                return True
            if isinstance(key_type, StructType):
                if search_for_uuid_column(key_type):
                    return True
            if isinstance(value_type, StructType):
                if search_for_uuid_column(value_type):
                    return True

    return False            

def rule_column_uuid_table(table: Table) -> Optional[Insight]:
    # Access the schema of the table
    uuid = search_for_uuid_column(table.schema())

    if uuid:
        meta = INSIGHT_META["UUID_COLUMN"]
        return Insight(
            code="UUID_COLUMN",
            table=qualified_table_name(table.name()),
            message=meta["message"],
            severity=meta["severity"],
            suggested_action=meta["suggested_action"]
        )
    return None

def rule_no_rows_table(table: Table) -> Optional[Insight]:
    empty: bool = False
    print(f"rule_no_rows_table: {table.name()}")
    if table.metadata.current_snapshot_id:
        paTable = table.inspect.snapshots().sort_by([('committed_at', 'descending')]).select(['summary', 'committed_at'])          
        result = dict(paTable.to_pydict()['summary'][0])
        total_records = int(result.get("total-records", -1))
        # snapshot summary doesn't always contains following 3 properties (total_records,total_file_size, total_data_files) hence getting from files meta, which is slower
        if total_records == -1:
            files_meta = table.inspect.files().select(['record_count'])
            total_records = pc.sum(files_meta['record_count']).as_py()
        empty = not (total_records > 0)
    else:
        empty = True

    if empty:
        meta = INSIGHT_META["NO_ROWS_TABLE"]
        return Insight(
            code="NO_ROWS_TABLE",
            table=qualified_table_name(table.name()),
            message=meta["message"],
            severity=meta["severity"],
            suggested_action=meta["suggested_action"]
        )
    return None

def rule_too_many_snapshot_table(table: Table) -> Optional[Insight]:
    snapshot_history = table.history()
    
    # Count the number of snapshots
    num_snapshots = len(list(snapshot_history))

    if num_snapshots > MAX_SNAPSHOTS_RECOMMENDED:
        meta = INSIGHT_META["SNAPSHOT_SPRAWL_TABLE"]
        return Insight(
            code="SNAPSHOT_SPRAWL_TABLE",
            table=qualified_table_name(table.name()),
            message=meta["message"].format(snapshots= num_snapshots, max_snapshots_recommended=MAX_SNAPSHOTS_RECOMMENDED),
            severity=meta["severity"],
            suggested_action=meta["suggested_action"]
        )
    return None


ALL_RULES = [
    rule_small_files, 
    rule_no_location, 
    rule_large_files, 
    rule_small_files_large_table, 
    rule_column_uuid_table, 
    rule_no_rows_table,
    rule_too_many_snapshot_table
]

