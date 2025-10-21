import time
import logging
import importlib
from threading import Timer
from fastapi import Request, HTTPException

from pyiceberg.table import Table
from app.lakeviewer import LakeView
from app.storage import get_storage
from app.models import BackgroundJob, InsightRun, JobSchedule, InsightRecord, ActiveInsight, QueuedTask
from app import config
from app.insights.runner import InsightsRunner

# --- Service Instances ---
lv = LakeView()

def get_runner():
    """
    FastAPI Dependency to provide a configured InsightsRunner.
    This manages the database connection lifecycle for a single request.
    """
    # 1. Create the storage instance
    storage = get_storage(model=InsightRun)
    insights_storage = get_storage(model=InsightRecord)
    active_insights_storage = get_storage(model=ActiveInsight)
    try:
        # 2. Connect to the database
        storage.connect()
        storage.ensure_table()
        insights_storage.connect()
        insights_storage.ensure_table()
        active_insights_storage.connect()
        active_insights_storage.ensure_table()
        # 3. Create the runner with its dependencies
        runner = InsightsRunner(lakeview=lv, run_storage=storage, insight_storage=insights_storage, active_insight_storage=active_insights_storage)
        # 4. Yield the runner to the endpoint function
        yield runner
    finally:
        # 5. This code runs after the response is sent, ensuring
        #    the connection is always closed.
        storage.disconnect()
        insights_storage.disconnect()
        active_insights_storage.disconnect()


# --- Authorization ---
authz_module = importlib.import_module(config.AUTHZ_MODULE)
authz_class = getattr(authz_module, config.AUTHZ_CLASS)
authz_ = authz_class()

# --- Storage Instances ---
background_job_storage = get_storage(model=BackgroundJob)
schedule_storage = get_storage(model=JobSchedule)
queued_task_storage = get_storage(model=QueuedTask)

# --- Caching ---
page_session_cache = {}
namespaces = []
ns_tables = {}

def clean_cache():
    """Remove expired entries from the cache."""
    current_time = time.time()
    keys_to_delete = [
        key for key, (_, timestamp) in page_session_cache.items()
        if current_time - timestamp > config.CACHE_EXPIRATION
    ]
    for key in keys_to_delete:
        del page_session_cache[key]
    Timer(60, clean_cache).start()

def refresh_namespace_and_tables():
    """Periodically refresh namespaces and tables."""
    global namespaces, ns_tables
    logging.info("Refreshing namespaces and tables...")
    namespaces = lv.get_namespaces()
    ns_tables = lv.get_all_table_names(namespaces)
    logging.info("Refresh complete.")
    Timer(3900, refresh_namespace_and_tables).start()

# --- Authentication Dependency ---
def check_auth(request: Request):
    return request.session.get("user")

# --- Table Loading Dependency ---
def load_table(table_id: str) -> Table:
    try:
        logging.info(f"Loading table {table_id}")
        return lv.load_table(table_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Table not found")

def get_table(request: Request, table_id: str):
    if config.AUTH_ENABLED:
        user = check_auth(request)
        if not user:
            raise HTTPException(status_code=401, detail="User not logged in")

    page_session_id = request.headers.get("X-Page-Session-ID")
    if not page_session_id:
        raise HTTPException(status_code=400, detail="Missing X-Page-Session-ID header")

    cache_key = f"{page_session_id}_{table_id}"
    if cache_key not in page_session_cache:
        tbl = load_table(table_id)
        page_session_cache[cache_key] = (tbl, time.time())
    
    tbl, _ = page_session_cache[cache_key]
    return tbl