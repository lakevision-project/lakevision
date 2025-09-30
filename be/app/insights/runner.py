from typing import List, Dict, Any, Optional, Set
from app.insights.rules import ALL_RULES_OBJECT
from app.insights.utils import qualified_table_name, get_namespace_and_table_name
from app.models import Insight, InsightRun, JobSchedule
from app.lakeviewer import LakeView
from app.storage.interface import StorageInterface

class InsightsRunner:
    def __init__(self, lakeview, storage_adapter: StorageInterface):
        self.lakeview = lakeview
        self.storage_adapter = storage_adapter

    def get_latest_run(self, namespace: str, size: int, table_name: str = None, showEmpty: bool = True):
        """
        Fetches a paginated list of insight runs from storage.
        If namespace is '*', it fetches runs from all namespaces.
        If showEmpty is False, it only returns runs where the results list is not empty.
        """
        # 1. Build the base WHERE clause and parameters
        params = {}
        where_clauses = []

        if namespace != '*':
            where_clauses.append('"namespace" = :namespace')
            params['namespace'] = namespace

        if table_name:
            where_clauses.append('"table_name" = :table_name')
            params['table_name'] = table_name

        # 2. Add the condition for the 'results' column if showEmpty is False
        if not showEmpty:
            # This condition filters out both NULL and empty JSON arrays '[]'
            where_clauses.append('"results" IS NOT NULL AND "results" != \'[]\'')

        # 3. Assemble the final query components
        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        # 4. Use a raw query for both count and results to ensure consistency
        count_query = f'SELECT COUNT(*) as total FROM "{self.storage_adapter.table_name}" {where_sql}'
        total_count_result = self.storage_adapter.execute_raw_select_query(count_query, params)
        total_count = total_count_result[0]['total'] if total_count_result else 0

        results_query = f'SELECT * FROM "{self.storage_adapter.table_name}" {where_sql} ORDER BY run_timestamp DESC LIMIT :limit'
        params['limit'] = size
        
        results = self.storage_adapter.find_by_raw_query(results_query, params)

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

        run_result = [
            insight
            for rule in ALL_RULES_OBJECT
            if rule.id in ids_to_run and (insight := rule.method(table))
        ]
        run = InsightRun(
            namespace=namespace,
            table_name=table_name,
            run_type=type,
            results=run_result,
            rules_requested=list(ids_to_run)
        )
        self.storage_adapter.save(run)

        return run_result

    def run_for_namespace(self, namespace: str, rule_ids: List[str] = None, recursive: bool = True, type: str = "manual") -> Dict[str, List[Insight]]:
        tables = self.lakeview.get_tables(namespace)
        results = []
        for t_ident in tables:
            qualified = qualified_table_name(t_ident)
            results.extend(self.run_for_table(qualified, rule_ids, type))
        if recursive:
            nested_namespaces = self.lakeview._get_nested_namespaces(namespace)
            for ns in nested_namespaces:
                ns_str = ".".join(ns)
                results.extend(self.run_for_namespace(ns_str, rule_ids, recursive=False, type=type))
        return results

    def run_for_lakehouse(self, rule_ids: List[str] = None, type: str = "manual") -> Dict[str, List[Insight]]:
        namespaces = self.lakeview.get_namespaces(include_nested=False)
        results = []
        for ns in namespaces:
            ns_str = ".".join(ns) if isinstance(ns, (tuple, list)) else str(ns)
            results.extend(self.run_for_namespace(ns_str, rule_ids, recursive=True, type=type))
        return results
