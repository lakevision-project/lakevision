from pyiceberg.table import Table 
from pyiceberg.types import StructType, ListType, MapType, UUIDType
from typing import Optional
from app.insights.utils import qualified_table_name
import yaml
import os
import pyarrow.compute as pc
from app.models import TableFile
from collections import defaultdict
from statistics import median
from typing import Dict

from app.models import Rule
from app.models import Insight

rules_yaml_path = os.path.join(os.path.dirname(__file__), "rules.yaml")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


ONE_GB_IN_BYTES = 1000**3  # 1 GB in bytes (1024**3 is the actual value)
LARGE_FILE_THRESHOLD_BYTES = _env_int("LV_RULE_LARGE_FILE_BYTES", ONE_GB_IN_BYTES)
SEVERAL_FILES = _env_int("LV_RULE_SMALL_FILES_MIN_COUNT", 100)
AVERAGE_SMALL_FILES_IN_BYTES = _env_int("LV_RULE_SMALL_FILES_AVG_BYTES", 100_000)
LARGE_TABLE_IN_BYTES = _env_int("LV_RULE_LARGE_TABLE_BYTES", 50 * ONE_GB_IN_BYTES)
AVERAGE_SMALL_FILES_LARGE_TABLES_IN_BYTES = _env_int("LV_RULE_SMALL_FILES_LARGE_TABLE_AVG_BYTES", 50_000)
MAX_SNAPSHOTS_RECOMMENDED = _env_int("LV_RULE_MAX_SNAPSHOTS", 500)
SKEWED_PARTITION_THRESHOLD_RATIO = _env_float("LV_RULE_SKEW_RATIO", 10)

# Load yaml at app startup
with open(rules_yaml_path) as f:
    INSIGHT_META = yaml.safe_load(f)


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
    large_files = [file_size for file_size in files if file_size >= LARGE_FILE_THRESHOLD_BYTES]
    num_large_files = len(large_files)
    if num_large_files >= 1:
        meta = INSIGHT_META["LARGE_FILES"]
        return Insight(
            code="LARGE_FILES",
            table=qualified_table_name(table.name()),
            message=meta["message"].format(
                num_files=len(files),
                avg_size=int(avg_size),
                num_large_files=num_large_files,
                max_size=LARGE_FILE_THRESHOLD_BYTES,
            ),
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


# Optional: compute Gini for rows
def gini(xs):
    if not xs: return 0.0
    xs = sorted(xs)
    n = len(xs)
    s = sum(xs)
    if s == 0: return 0.0
    cum = 0
    for i, x in enumerate(xs, 1):
        cum += i * x
    return (2*cum)/(n*s) - (n+1)/n

def rule_skewed_or_largest_partitions_table(table: Table) -> Optional[Insight]:
    partition_spec = table.spec()
    
    # Table has partitions
    if partition_spec.fields:
        try:
            files = [TableFile.from_task(task) for task in table.scan().plan_files()]
            partition_summary = defaultdict(lambda: {"records": 0, "size_bytes": 0})
            for file in files:
                # Create a unique, sortable key for the partition
                # Ensure the partition filters are sorted consistently
                partition_key = tuple(sorted([(col.name, col.value) for col in file.partition]))
                
                # Aggregate records and size_bytes
                partition_summary[partition_key]["records"] += file.records
                partition_summary[partition_key]["size_bytes"] += file.size_bytes

            partition_list = []
            if len(partition_summary)>0:
                for partition_key, summary_values in partition_summary.items():
                    partition_list.append({
                        "partition_key": partition_key,
                        "records": summary_values["records"],
                        "size_bytes": summary_values["size_bytes"]
                    })

                partitions_size = len(partition_summary)
                records_by_partition = [v["records"] for v in partition_summary.values()]
                size_by_partition = [v["size_bytes"] for v in partition_summary.values()]
                median_records = median(records_by_partition)
                median_size = median(size_by_partition)
                # Condition that checks for a significant positive skew in the data. It evaluates to True if the largest value in your dataset is more than X times greater than the median value.
                skewed_records = max(records_by_partition)/median_records > SKEWED_PARTITION_THRESHOLD_RATIO
                skewed_size = max(size_by_partition)/median_size > SKEWED_PARTITION_THRESHOLD_RATIO

                largest_records = max(records_by_partition)
                largest_size = max(size_by_partition)
        
                meta = INSIGHT_META["SKEWED_OR_LARGEST_PARTITIONS_TABLE"]
                if skewed_records or skewed_size:
                    return Insight(
                        code="SKEWED_OR_LARGEST_PARTITIONS_TABLE",
                        table=qualified_table_name(table.name()),
                        message=meta["message"].format(partitions=int(partitions_size), skew_ratio=int(SKEWED_PARTITION_THRESHOLD_RATIO), 
                                                    median_size=int(median_size), largest_size=str(largest_size),
                                                    median_records=int(median_records), largest_records=str(largest_records)),
                        severity=meta["severity"],
                        suggested_action=meta["suggested_action"]
                    )

        except Exception as e:
            print(str(e))
            raise Exception(f"There was a problem with your request: {e}") from e
        
    return None


ALL_RULES = [
    rule_small_files, 
    rule_no_location, 
    rule_large_files, 
    rule_small_files_large_table, 
    rule_column_uuid_table, 
    rule_no_rows_table,
    rule_too_many_snapshot_table,
    rule_skewed_or_largest_partitions_table
]

ALL_RULES_OBJECT = [
    Rule("SMALL_FILES", "Many small files", "Table with many small data files could improve performance with fewer, larger files. Use optimize or less partitioning to improve.", rule_small_files),
    Rule("NO_LOCATION", "No location", "The table has no location set", rule_no_location),
    Rule("LARGE_FILES", "Large files", "Table with single/avg parquet file size >1GB, leading to large memory overhead and degraded process distribution. Rewrite table with a lower target maximum file size.", rule_large_files),
    Rule("SMALL_FILES_LARGE_TABLE", "Large table with small files", "Table with <50MB per parquet file and larger than 50GB total. Use optimize or less partitioning to improve.", rule_small_files_large_table),
    Rule("UUID_COLUMN", "UUID type not universally supported", "UUID column type may not be supported in all environments, especially Spark and older Presto", rule_column_uuid_table),
    Rule("NO_ROWS_TABLE", "Empty table", "Table has been declared but has no data. Possibly intentional.", rule_no_rows_table),
    Rule("SNAPSHOT_SPRAWL_TABLE", "Too many snapshots", "A high snapshot count for a table creates memory and process overhead for the catalog service and catalog results processing on clients. Expire snapshots or adjust snapshot age configuration if practical.", rule_too_many_snapshot_table),
    Rule("SKEWED_OR_LARGEST_PARTITIONS_TABLE", "Large partition", "The table contains one partition that is considerably larger than the rest. Evaluate if the table can be repartitioned by another column/criteria.", rule_skewed_or_largest_partitions_table)
]
