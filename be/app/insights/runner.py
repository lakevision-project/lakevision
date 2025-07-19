from typing import List, Dict, Any, Optional
from .rules import ALL_RULES, Insight
from .utils import qualified_table_name

class InsightsRunner:
    def __init__(self, lakeview):
        self.lakeview = lakeview

    def run_for_table(self, table_identifier) -> List[Insight]:
        table = self.lakeview.load_table(table_identifier)
        return [insight for rule in ALL_RULES if (insight := rule(table))]

    def run_for_namespace(self, namespace: str, recursive: bool = True) -> Dict[str, List[Insight]]:
        tables = self.lakeview.get_tables(namespace)
        results = {}
        for t_ident in tables:
            qualified = qualified_table_name(t_ident)
            results[qualified] = self.run_for_table(t_ident)
        if recursive:
            nested_namespaces = self.lakeview._get_nested_namespaces(namespace)
            for ns in nested_namespaces:
                ns_str = ".".join(ns)
                results.update(self.run_for_namespace(ns_str, recursive=False))
        return results

    def run_for_lakehouse(self) -> Dict[str, List[Insight]]:
        namespaces = self.lakeview.get_namespaces(include_nested=False)
        results = {}
        for ns in namespaces:
            ns_str = ".".join(ns) if isinstance(ns, (tuple, list)) else str(ns)
            results.update(self.run_for_namespace(ns_str, recursive=True))
        return results
