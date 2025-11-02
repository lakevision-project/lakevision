import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import JSONResponse
from app import config
from app.utils import get_bool_env
from app.api_utils import CleanJSONResponse
from app.exceptions import LVException
from app.dependencies import (
    background_job_storage, schedule_storage,
    clean_cache, refresh_namespace_and_tables,
    queued_task_storage
)
from app.routers import auth, tables, insights, jobs

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Check if the LAKEVISION_HEALTH_ENABLED is set to true.
HEALTH_ENABLED = get_bool_env('LAKEVISION_HEALTH_ENABLED')
if not HEALTH_ENABLED:
    logging.warning("LAKEVISION_HEALTH_ENABLED is not set to 'true'. Health check and job scheduling features will be disabled.")

# --- Application Lifespan (Startup/Shutdown Events) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Application startup...")
    # Connect to databases and create tables
    if HEALTH_ENABLED:
        background_job_storage.connect()
        background_job_storage.ensure_table()
        schedule_storage.connect()
        schedule_storage.ensure_table()
        queued_task_storage.connect()
        queued_task_storage.ensure_table()
    
    # Start periodic maintenance tasks
    refresh_namespace_and_tables()
    clean_cache()
    
    print("Startup complete.")
    yield
    print("Application shutdown...")
    if HEALTH_ENABLED:
        background_job_storage.disconnect()
        schedule_storage.disconnect()
        queued_task_storage.disconnect()
    print("Shutdown complete.")

# --- FastAPI App Initialization ---
app = FastAPI(
    default_response_class=CleanJSONResponse,
    lifespan=lifespan
)

# --- Middleware ---
app.add_middleware(SessionMiddleware, secret_key=config.SECRET_KEY, max_age=7200)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Exception Handlers ---
@app.exception_handler(LVException)
async def lv_exception_handler(request: Request, exc: LVException):
    return JSONResponse(
        status_code=418,  # Using 418 as in the original code
        content={"name": exc.name, "message": exc.message},
    )

# --- Include Routers ---
app.include_router(auth.router)
app.include_router(tables.router)
if HEALTH_ENABLED:
    app.include_router(insights.router)
    app.include_router(jobs.router)