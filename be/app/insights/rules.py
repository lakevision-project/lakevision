from pyiceberg.table import Table
from typing import Optional
from dataclasses import dataclass
from app.insights.utils import qualified_table_name
import yaml
import os

rules_yaml_path = os.path.join(os.path.dirname(__file__), "rules.yaml")

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
    avg_size = sum(files) / len(files)
    if len(files) > 100 and avg_size < 100_000:
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

ALL_RULES = [rule_small_files, rule_no_location]

