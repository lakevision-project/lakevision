import time
import logging
from collections import OrderedDict
import importlib
import importlib.util
import sys
from pathlib import Path
from threading import Timer
from fastapi import Request, HTTPException

from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table import Table
from app.lakeviewer import LakeView
from app.storage import get_storage
from app.models import BackgroundJob, InsightRun, JobSchedule, InsightRecord, ActiveInsight, QueuedTask
from app import config
from app.insights.runner import InsightsRunner

logger = logging.getLogger(__name__)

DEFAULT_AUTHZ_MODULE = "app.authz"
DEFAULT_AUTHZ_CLASS = "Authz"


def _load_default_authz_class():
    """Load the built-in authz.py implementation directly from disk."""
    authz_py = Path(__file__).resolve().parent / "authz.py"
    if not authz_py.exists():
        logger.error("Bundled authz.py not found at %s", authz_py)
        return None

    spec = importlib.util.spec_from_file_location("app._default_authz", authz_py)
    if spec is None or spec.loader is None:
        logger.error("Unable to create module spec for bundled authz.py")
        return None

    module = importlib.util.module_from_spec(spec)
    sys.modules["app._default_authz"] = module
    spec.loader.exec_module(module)

    fallback_class = getattr(module, DEFAULT_AUTHZ_CLASS, None)
    if fallback_class is None:
        logger.error(
            "Bundled authz.py does not define class '%s'",
            DEFAULT_AUTHZ_CLASS,
        )
        return None

    logger.info("Loaded bundled authorization class from %s", authz_py)
    return fallback_class


def _load_authz_class():
    """Dynamically load an authorization class, preferring configured plugins."""
    try:
        authz_module = importlib.import_module(config.AUTHZ_MODULE)
        authz_class = getattr(authz_module, config.AUTHZ_CLASS)
        logger.info(
            "Loaded authorization plugin %s.%s",
            config.AUTHZ_MODULE,
            config.AUTHZ_CLASS,
        )
        return authz_class
    except Exception as exc:
        using_defaults = (
            config.AUTHZ_MODULE == DEFAULT_AUTHZ_MODULE
            and config.AUTHZ_CLASS == DEFAULT_AUTHZ_CLASS
        )
        if using_defaults:
            logger.warning(
                "Default authorization import (%s.%s) failed; "
                "falling back to bundled authz.py. Error: %s",
                config.AUTHZ_MODULE,
                config.AUTHZ_CLASS,
                exc,
            )
            fallback_class = _load_default_authz_class()
            if fallback_class:
                return fallback_class
        logger.exception(
            "Unable to initialize authorization plugin '%s.%s'",
            config.AUTHZ_MODULE,
            config.AUTHZ_CLASS,
        )
        raise


# --- Service Instances ---
lv = LakeView()

# --- Authorization ---
authz_class = _load_authz_class()
authz_ = authz_class()

# --- Storage Instances ---
background_job_storage = get_storage(model=BackgroundJob)
schedule_storage = get_storage(model=JobSchedule)
queued_task_storage = get_storage(model=QueuedTask)
insight_run_storage = get_storage(model=InsightRun)
insight_record_storage = get_storage(model=InsightRecord)
active_insight_storage = get_storage(model=ActiveInsight)

def get_runner():
    """
    FastAPI Dependency to provide a configured InsightsRunner
    within a proper database session/transaction.
    """
    # 1. Create the storage instance - REMOVED
    # 2. Connect to the database - MOVED TO API.PY
    
    # We now use db_session() for the request's unit of work,
    # just like get_atomic_job does in worker.py.
    try:
        with insight_run_storage.db_session(), \
             insight_record_storage.db_session(), \
             active_insight_storage.db_session():
            
            # 3. Create the runner (this is cheap)
            runner = InsightsRunner(
                lakeview=lv, 
                run_storage=insight_run_storage, 
                insight_storage=insight_record_storage,
                active_insight_storage=active_insight_storage
            )
            # 4. Yield the runner to the endpoint function
            yield runner
            
    except Exception as e:
        # 5. This block catches errors during the session
        logging.error(f"Error in get_runner DB session: {e}")
        # The 'with' block will handle rollback.
        raise HTTPException(status_code=500, detail="Database session error")

# --- Caching ---
# Bounded, insertion-ordered LRU. The key includes the authenticated user so two
# clients cannot share a cached Table by colliding on X-Page-Session-ID, and the
# bound stops an unauthenticated caller from growing the dict without limit by
# sending random session IDs (memory-exhaustion DoS).
page_session_cache: "OrderedDict[tuple, tuple]" = OrderedDict()
namespaces = []
ns_tables = {}

def clean_cache():
    """Remove expired entries from the cache."""
    current_time = time.time()
    keys_to_delete = [
        key for key, (_, timestamp) in list(page_session_cache.items())
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
    """Load a table, distinguishing "missing" from "could not be parsed".

    Previously any failure became 404 "Table not found", so a table the catalog
    lists but pyiceberg cannot parse looked identical to a typo'd name -- and the
    real cause never reached the logs or the UI.
    """
    try:
        logging.info("Loading table %s", table_id)
        return lv.load_table(table_id)
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail="Table not found")
    except HTTPException:
        raise
    except Exception as exc:
        # The catalog knows the table but its metadata could not be read -- e.g.
        # a field pyiceberg validates more strictly than the Iceberg spec
        # requires. Report it as a server-side problem and log the detail.
        logging.exception("Failed to load table %s", table_id)
        raise HTTPException(
            status_code=502,
            detail=(
                f"Table metadata could not be read: {type(exc).__name__}. "
                "See server logs for details."
            ),
        )

def get_table(request: Request, table_id: str):
    user = None
    if config.AUTH_ENABLED:
        user = check_auth(request)
        if not user:
            raise HTTPException(status_code=401, detail="User not logged in")

    page_session_id = request.headers.get("X-Page-Session-ID")
    if not page_session_id:
        raise HTTPException(status_code=400, detail="Missing X-Page-Session-ID header")

    # X-Page-Session-ID is chosen by the client, so it is scoped by the
    # authenticated user rather than trusted on its own.
    cache_key = (user, page_session_id, table_id)
    cached = page_session_cache.get(cache_key)
    if cached is not None:
        page_session_cache.move_to_end(cache_key)
        return cached[0]

    tbl = load_table(table_id)
    page_session_cache[cache_key] = (tbl, time.time())
    while len(page_session_cache) > config.PAGE_CACHE_MAX_ENTRIES:
        page_session_cache.popitem(last=False)
    return tbl
