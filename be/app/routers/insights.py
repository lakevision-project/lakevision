import logging
from typing import List, Optional
from fastapi import APIRouter, Query, Depends

from app.insights.runner import InsightsRunner
from app.insights.rules import ALL_RULES_OBJECT
from app.dependencies import  get_runner
from app.exceptions import LVException
from app.models import RuleOut

router = APIRouter()

@router.get("/api/namespaces/{namespace}/insights")
@router.get("/api/namespaces/{namespace}/{table_name}/insights")
def get_latest_table_insights(
    namespace: str,
    table_name: Optional[str] = None,
    size: int = Query(5, ge=1),
    showEmpty: bool = False,
    runner: InsightsRunner = Depends(get_runner)
):
    # The call to the runner is updated to pass the new parameters
    paginated_data = runner.get_latest_run(
        namespace=namespace,
        table_name=table_name,
        size=size,
        showEmpty=showEmpty
    )
    return [run.__dict__ for run in paginated_data]

@router.get("/api/lakehouse/insights/rules", response_model=List[RuleOut])
def get_insight_rules():
    return ALL_RULES_OBJECT