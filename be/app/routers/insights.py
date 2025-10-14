import logging
from typing import List, Optional
from fastapi import APIRouter, Query, Depends

from app.insights.runner import InsightsRunner
from app.insights.rules import ALL_RULES_OBJECT
from app.dependencies import get_runner
from app.exceptions import LVException
from app.models import RuleOut, RuleSummaryOut, InsightRun, InsightRunOut

router = APIRouter()

@router.get("/api/namespaces/{namespace}/insights", response_model=List[InsightRunOut]) # Use dict for flexibility
@router.get("/api/namespaces/{namespace}/{table_name}/insights", response_model=List[InsightRunOut])
def get_latest_table_insights(
    namespace: str,
    table_name: Optional[str] = None,
    size: int = Query(5, ge=1),
    showEmpty: bool = True,
    runner: InsightsRunner = Depends(get_runner)
):
    paginated_data = runner.get_latest_run(
        namespace=namespace,
        table_name=table_name,
        size=size,
        showEmpty=showEmpty
    )
    return [run.__dict__ for run in paginated_data]

@router.get(
    "/api/namespaces/{namespace}/insights/summary",
    response_model=List[RuleSummaryOut],
    summary="Get Insight Summary by Rule"
)
@router.get(
    "/api/namespaces/{namespace}/{table_name}/insights/summary",
    response_model=List[RuleSummaryOut],
    summary="Get Insight Summary by Rule for a Table"
)
def get_insights_summary(
    namespace: str,
    table_name: Optional[str] = None,
    rules: Optional[List[str]] = Query(None),
    runner: InsightsRunner = Depends(get_runner)
):
    """
    Provides an aggregated summary of insights, grouped by rule.
    This is useful for dashboard views at the namespace or lakehouse level.
    """
    summary_data = runner.get_summary_by_rule(
        namespace=namespace,
        table_name=table_name,
        rule_codes=rules
    )
    return summary_data

@router.get("/api/lakehouse/insights/rules", response_model=List[RuleOut])
def get_insight_rules():
    return ALL_RULES_OBJECT