import logging
from typing import List, Optional
from fastapi import APIRouter, Query, Depends

from app.insights.runner import InsightsRunner
from app.insights.rules import ALL_RULES_OBJECT
from app.dependencies import  get_runner
from app.exceptions import LVException
from app.models import RuleOut

router = APIRouter()

@router.get("/api/tables/{table_id}/insights/latest")
def get_latest_table_insights(table_id: str, size: int = Query(5, ge=1), runner: InsightsRunner = Depends(get_runner)):
    paginated_data = runner.get_latest_run(table_id, size=size)
    return [run.__dict__ for run in paginated_data]

@router.get("/api/tables/{table_id}/insights")
def get_table_insights(table_id: str, rule_ids: Optional[List[str]] = Query(None, alias="rule_ids"), runner: InsightsRunner = Depends(get_runner)):
    try:
        results = runner.run_for_table(table_id, rule_ids)
        return [insight.__dict__ for insight in results]
    except Exception as e:
        logging.error(f"Insights error for table {table_id}: {str(e)}")
        raise LVException("insight", f"Failed to compute insights: {e}")

@router.get("/api/namespaces/{namespace}/insights")
def get_namespace_insights(namespace: str, recursive: bool = Query(True), runner: InsightsRunner = Depends(get_runner)):
    try:
        results = runner.run_for_namespace(namespace, recursive=recursive)
        return {k: [ins.__dict__ for ins in v] for k, v in results.items()}
    except Exception as e:
        logging.error(f"Insights error for namespace {namespace}: {str(e)}")
        raise LVException("insight", f"Failed to compute insights: {e}")

@router.get("/api/lakehouse/insights/rules", response_model=List[RuleOut])
def get_insight_rules():
    return ALL_RULES_OBJECT

@router.get("/api/lakehouse/insights")
def get_lakehouse_insights(runner: InsightsRunner = Depends(get_runner)):
    try:
        results = runner.run_for_lakehouse()
        return {k: [ins.__dict__ for ins in v] for k, v in results.items()}
    except Exception as e:
        logging.error(f"Insights error for lakehouse: {str(e)}")
        raise LVException("insight", f"Failed to compute insights: {e}")