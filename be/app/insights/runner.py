from typing import List, Dict, Any, Optional, Set
from collections import defaultdict

from app.insights.rules import ALL_RULES_OBJECT, INSIGHT_META
from app.insights.utils import qualified_table_name, get_namespace_and_table_name
from app.models import Insight, InsightRun, InsightRecord, JobSchedule, InsightRunOut, ActiveInsight, InsightOccurrence, RuleSummaryOut
from app.lakeviewer import LakeView
from app.storage.interface import StorageInterface
from sqlalchemy import text, bindparam

class InsightsRunner:
    def __init__(self, lakeview, 
                 run_storage: StorageInterface[InsightRun], 
                 insight_storage: StorageInterface[InsightRecord],
                 active_insight_storage: StorageInterface[ActiveInsight]):
        self.lakeview = lakeview
        self.run_storage = run_storage
        self.insight_storage = insight_storage
        self.active_insight_storage = active_insight_storage

    def get_latest_run(self, namespace: str, size: int, table_name: str = None, showEmpty: bool = True) -> List[InsightRun]:
        """
        Fetches a paginated list of insight runs and reconstructs them with their results.
        If showEmpty is False, it only returns runs that have at least one insight result.
        """
        params = {}
        where_clauses = []

        if namespace != '*':
            where_clauses.append(f'"{self.run_storage.table_name}"."namespace" = :namespace')
            params['namespace'] = namespace

        if table_name:
            where_clauses.append(f'"{self.run_storage.table_name}"."table_name" = :table_name')
            params['table_name'] = table_name

        if not showEmpty:
            where_clauses.append(
                f'EXISTS (SELECT 1 FROM "{self.insight_storage.table_name}" WHERE "{self.insight_storage.table_name}"."run_id" = "{self.run_storage.table_name}"."id")'
            )

        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        results_query = f'SELECT * FROM "{self.run_storage.table_name}" {where_sql} ORDER BY run_timestamp DESC LIMIT :limit'
        params['limit'] = size
        runs = self.run_storage.find_by_raw_query(results_query, params)

        if not runs:
            return []

        run_ids = [run.id for run in runs]
        related_insights = self.insight_storage.get_by_attributes({"run_id": run_ids})

        insights_by_run_id = defaultdict(list)
        for insight in related_insights:
            insights_by_run_id[insight.run_id].append(insight)

        response_models = []
        for run in runs:
            run_data = run.__dict__
            run_data['results'] = insights_by_run_id.get(run.id, [])
            response_models.append(InsightRunOut(**run_data))
            
        return response_models

    def get_summary_by_rule(self, 
                            namespace: str, 
                            table_name: Optional[str] = None,
                            rule_codes: Optional[List[str]] = None
                            ) -> List[Dict[str, Any]]:
        criteria = {}
        if namespace != '*':
            criteria['namespace'] = namespace
        if table_name:
            criteria['table_name'] = table_name
        if rule_codes:
            criteria['code'] = rule_codes
        
        active_insights: List[ActiveInsight] = self.active_insight_storage.get_by_attributes(criteria)
        
        if not active_insights:
            return []

        grouped_data = defaultdict(list)
        for insight in active_insights:
            group_key = (insight.code, insight.namespace)
            grouped_data[group_key].append(insight)

        final_summary = []
        for (code, ns), occurrences in grouped_data.items():
            suggested_action = occurrences[0].suggested_action

            occurrence_models = [
                InsightOccurrence(
                    table_name=occ.table_name,
                    severity=occ.severity,
                    message=occ.message,
                    timestamp=occ.last_seen_timestamp
                ) for occ in occurrences
            ]

            final_summary.append(
                RuleSummaryOut(
                    code=code,
                    namespace=ns,
                    suggested_action=suggested_action,
                    occurrences=occurrence_models
                )
            )
            
        return final_summary

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

        run_result: List[Insight] = [
            insight
            for rule in ALL_RULES_OBJECT
            if rule.id in ids_to_run and (insight := rule.method(table))
        ]

        run = InsightRun(
            namespace=namespace,
            table_name=table_name,
            run_type=type,
            rules_requested=list(ids_to_run)
        )
        self.run_storage.save(run)

        if run_result:
            insight_records = [
                InsightRecord(run_id=run.id, **insight.__dict__) for insight in run_result
            ]
            self.insight_storage.save_many(insight_records)

        self.active_insight_storage.delete_by_attributes({
            "namespace": namespace,
            "table_name": table_name,
            "code": list(ids_to_run)
        })

        if run_result:
            new_active_insights = [
                ActiveInsight(
                    table_name=table_name,
                    code=insight.code,
                    namespace=namespace,
                    severity=insight.severity,
                    message=insight.message,
                    suggested_action=insight.suggested_action,
                    last_seen_run_id=run.id
                )
                for insight in run_result
            ]
            self.active_insight_storage.save_many(new_active_insights)

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