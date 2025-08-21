from pyiceberg.table import Table
from typing import Optional
from dataclasses import dataclass
from app.insights.utils import qualified_table_name
import yaml
import os

rules_yaml_path = os.path.join(os.path.dirname(__file__), "rules.yaml")

SEVERAL_FILES = 100
AVERAGE_SMALL_FILES_IN_BYTES = 100_000
ONE_GB_IN_BYTES = 1000**3  # 1 GB in bytes (1024**3 is the actual value)
LARGE_TABLE_IN_BYTES= 50 * ONE_GB_IN_BYTES
AVERAGE_SMALL_FILES_LARGE_TABLES_IN_BYTES = 50_000

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

ALL_RULES = [rule_small_files, rule_no_location, rule_large_files, rule_small_files_large_table]

