from typing import List, Dict, Any, Optional, Set
from app.insights.rules import Insight, ALL_RULES_OBJECT, InsightRun
from app.insights.utils import qualified_table_name, get_namespace_and_table_name
from app.storage import run_storage
from app.insights.job_schedule import JobSchedule
from app.lakeviewer import LakeView

def execute_job(schedule: JobSchedule):
    """
    Takes a schedule object and executes the insight run.
    """
   
    lv = LakeView()
    runner = InsightsRunner(lv)

    target = schedule.namespace
    if schedule.table_name:
        target += f".{schedule.table_name}"
    print(f"Executing job for target '{target}'")

    raw_insights = runner.run_for_table(target, rule_ids=schedule.rules_requested, type="auto")

    print(f"Execution finished for schedule {schedule.id}. Found {len(raw_insights)} insights.")

class InsightsRunner:
    def __init__(self, lakeview):
        self.lakeview = lakeview

    def get_latest_run(self, table_identifier: str, size: int):
        """
        Fetches a paginated list of insight runs from storage.
        """
        namespace, table_name = get_namespace_and_table_name(table_identifier)
        run_storage.connect()
        criteria = {
            "namespace": namespace,
            "table_name": table_name
        }

        total_count = run_storage.get_aggregate("COUNT", "*", criteria)
        
        results = run_storage.get_by_attributes(criteria, limit=size)
        
        run_storage.disconnect()

        return results

    def run_for_table(self, table_identifier, rule_ids: List[str] = None, type: str = "manual") -> List[Insight]:
        table = self.lakeview.load_table(table_identifier)
    
        all_valid_ids: Set[str] = {rule.id for rule in ALL_RULES_OBJECT}
        
        ids_to_run: Set[str]
        
        if rule_ids is None:
            ids_to_run = all_valid_ids
        else:
            provided_ids = set(rule_ids)
            invalid_ids = provided_ids - all_valid_ids
            
            if invalid_ids:
                raise ValueError(f"Invalid rule IDs provided: {', '.join(sorted(invalid_ids))}")
        
            ids_to_run = provided_ids

        namespace, table_name = get_namespace_and_table_name(table_identifier)
        print("here")

        run_result = [
            insight
            for rule in ALL_RULES_OBJECT
            if rule.id in ids_to_run and (insight := rule.method(table))
        ]
        print(run_result)
        run = InsightRun(
            namespace=namespace,
            table_name=table_name,
            run_type=type,
            results=run_result,
            rules_requested=list(ids_to_run)
        )
        run_storage.connect()
        run_storage.save(run)
        run_storage.disconnect()

        return run_result

    def run_for_namespace(self, namespace: str, rule_ids: List[str] = None, recursive: bool = True, type: str = "manual") -> Dict[str, List[Insight]]:
        tables = self.lakeview.get_tables(namespace)
        results = {}
        for t_ident in tables:
            qualified = qualified_table_name(t_ident)
            results[qualified] = self.run_for_table(t_ident, rule_ids, type)
        if recursive:
            nested_namespaces = self.lakeview._get_nested_namespaces(namespace)
            for ns in nested_namespaces:
                ns_str = ".".join(ns)
                results.update(self.run_for_namespace(ns_str, rule_ids, recursive=False, type=type))
        return results

    def run_for_lakehouse(self, rule_ids: List[str] = None, type: str = "manual") -> Dict[str, List[Insight]]:
        namespaces = self.lakeview.get_namespaces(include_nested=False)
        results = {}
        for ns in namespaces:
            ns_str = ".".join(ns) if isinstance(ns, (tuple, list)) else str(ns)
            results.update(self.run_for_namespace(ns_str, rule_ids, recursive=True, type=type))
        return results
