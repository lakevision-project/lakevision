from fastapi import FastAPI, Depends, HTTPException, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from authlib.integrations.starlette_client import OAuth
from starlette.config import Config
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import JSONResponse
from lakeviewer import LakeView
from app.insights.runner import InsightsRunner
from fastapi import Query
from typing import Generator
from pyiceberg.table import Table
import time, os, requests
from threading import Timer
from pydantic import BaseModel
import importlib
import logging

AUTH_ENABLED        = True if os.getenv("PUBLIC_AUTH_ENABLED", '')=='true' else False
CLIENT_ID           = os.getenv("PUBLIC_OPENID_CLIENT_ID", '')
OPENID_PROVIDER_URL = os.getenv("PUBLIC_OPENID_PROVIDER_URL", '')
REDIRECT_URI        = os.getenv("PUBLIC_REDIRECT_URI", '')
CLIENT_SECRET       = os.getenv("OPEN_ID_CLIENT_SECRET", '')
TOKEN_URL           = OPENID_PROVIDER_URL+"/token"
SECRET_KEY          = os.getenv("SECRET_KEY", "@#dsfdds1112")

AUTHZ_MODULE = os.getenv("AUTHZ_MODULE_NAME") or "authz"
AUTHZ_CLASS  = os.getenv("AUTHZ_CLASS_NAME") or "Authz"

class LVException(Exception):
    def __init__(self, name: str, message: str):
        self.name = name
        self.message = message

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

app = FastAPI()
lv = LakeView()

authz_module = importlib.import_module(AUTHZ_MODULE)
authz_class = getattr(authz_module, AUTHZ_CLASS)
authz_ = authz_class()

app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY, max_age=7200) # 7200 = 2 hrs
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

config = Config(environ={
    'AUTHLIB_INSECURE_TRANSPORT': '1',  # Only for local development; remove in production
})
oauth = OAuth(config)

oauth.register(
    name="openid",
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    server_metadata_url=f"{OPENID_PROVIDER_URL}/.well-known/openid-configuration",
    client_kwargs={
        "scope": "email"
    }
)

page_session_cache = {}
CACHE_EXPIRATION = 4 * 60 # 4 minutes
namespaces = None
ns_tables = None

@app.exception_handler(LVException)
async def lv_exception_handler(request: Request, exc: LVException):
    return JSONResponse(
        status_code=418,
        content={"message": exc.message, "name": exc.name},
    )

def load_table(table_id: str) -> Table: #Generator[Table, None, None]:    
    try:
        logging.info(f"Loading table {table_id}")
        table = lv.load_table(table_id)
        return table  # This makes it a generator
    except Exception as e:
        raise HTTPException(status_code=404, detail="Table not found")
    finally:
        # Optional cleanup
        logging.info(f"Finished with loading table {table_id}")

# Dependency to load the table only once
def get_table(request: Request, table_id: str):
    if AUTH_ENABLED:
        user = check_auth(request)
        if not user:
            raise HTTPException(status_code=401, detail="User Not logged in")
    page_session_id = request.headers.get("X-Page-Session-ID")    
    if not page_session_id:
        raise HTTPException(status_code=400, detail="Missing X-Page-Session-ID header")
    cache_key = f"{page_session_id}_{table_id}"

    if cache_key not in page_session_cache:
        tbl = load_table(table_id)
        page_session_cache[cache_key] = (tbl, time.time())
    else:
        tbl, timestamp = page_session_cache[cache_key]
    return tbl    

def check_auth(request: Request):
    user = request.session.get("user")
    if user:
        return JSONResponse(user)
    return None

@app.get("/api/namespaces")
def read_namespaces(refresh=False):    
    ret = []
    global namespaces
    if refresh or len(namespaces)==0:
        namespaces = lv.get_namespaces()
    for idx, ns in enumerate(namespaces):
        ret.append({"id": idx, "text": ".".join(ns)})        
    return ret

@app.get("/api/namespaces/{namespace}/special-properties")
def read_namespaces_special_properties(namespace: str):
    return authz_.get_namespace_special_properties(namespace)

@app.get("/api/tables/{table_id}/special-properties")
def read_table_special_properties(table_id: str):
    return authz_.get_table_special_properties(table_id)

@app.get("/api/tables/{table_id}/snapshots")
def read_table_snapshots(table: Table = Depends(get_table)):
    return lv.get_snapshot_data(table)

@app.get("/api/tables/{table_id}/partitions", status_code=status.HTTP_200_OK)
def read_table_partitions(request: Request, response: Response, table_id: str, table: Table = Depends(get_table)):
    if not authz_.has_access(request, response, table_id):        
        return
    return lv.get_partition_data(table)


@app.get("/api/tables/{table_id}/sample", status_code=status.HTTP_200_OK)    
def read_sample_data(request: Request, response: Response, table_id: str, sql = None, table: Table = Depends(get_table), sample_limit: int=100):
    if not authz_.has_access(request, response, table_id):        
        return
    try:    
        res =  lv.get_sample_data(table, sql, sample_limit)
        return res
    except Exception as e:
        logging.error(str(e))
        raise LVException("err", str(e))

@app.get("/api/tables/{table_id}/schema")    
def read_schema_data(table: Table = Depends(get_table)):
    return lv.get_schema(table)

@app.get("/api/tables/{table_id}/summary")    
def read_summary_data(table: Table = Depends(get_table)):
    return lv.get_summary(table)

@app.get("/api/tables/{table_id}/properties")    
def read_properties_data(table: Table = Depends(get_table)):
    return lv.get_properties(table)

@app.get("/api/tables/{table_id}/partition-specs")    
def read_partition_specs(table: Table = Depends(get_table)):
    return lv.get_partition_specs(table)

@app.get("/api/tables/{table_id}/sort-order")    
def read_sort_order(table: Table = Depends(get_table)):
    return lv.get_sort_order(table)

@app.get("/api/tables/{table_id}/data-change")    
def read_data_change(table: Table = Depends(get_table)):
    return lv.get_data_change(table)

@app.get("/")
def root(request: Request):
    if AUTH_ENABLED:
        user = check_auth(request)
        if not user:
            return {"message": "You are not logged in."}
    return "Hello, no auth enabled"
       
@app.get("/api/tables")
def read_tables(namespace: str = None, refresh=False, user=Depends(check_auth)):    
    if AUTH_ENABLED and not user:
        raise HTTPException(status_code=401, detail="User Not logged in")
    ret = []
    if not namespace:
        global namespaces
        global ns_tables        
        if refresh:            
            namespaces = lv.get_namespaces()
            ns_tables = lv.get_all_table_names(namespaces)
        for namespace, tables in ns_tables.items():           
            for idx, table in enumerate(tables):            
                ret.append({"id": idx, "text": table, "namespace": ".".join(namespace)})
        return ret
    for idx, table in enumerate(lv.get_tables(namespace)):
        ret.append({"id": idx, "text": table[-1], "namespace": namespace})        
    return ret

@app.get("/api/login")
async def login(request: Request):
    redirect_uri = REDIRECT_URI
    return await oauth.openid.authorize_redirect(request, redirect_uri)

class TokenRequest(BaseModel):
    code: str

@app.post("/api/auth/token")
def get_token(request: Request, tokenReq: TokenRequest):
    data = {
        "grant_type": "authorization_code",
        "code": tokenReq.code,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri": REDIRECT_URI,
    }    
    response = requests.post(TOKEN_URL, data=data)    
    if response.status_code != 200:
        raise HTTPException(status_code=400, detail="Failed to exchange code for token")
    r2 = requests.get(f"{OPENID_PROVIDER_URL}/userinfo?access_token={response.json()['access_token']}")
    user_email = r2.json()['email'] 
    request.session['user'] = user_email
    return JSONResponse(user_email)  

@app.get("/api/logout")
def logout(request: Request):
    request.session.pop("user", None)
    return RedirectResponse(REDIRECT_URI)

# Insights API
@app.get("/api/tables/{table_id}/insights")
def get_table_insights(table_id: str, request: Request):
    runner = InsightsRunner(lv)
    try:
        results = runner.run_for_table(table_id)
        return [insight.__dict__ for insight in results]
    except Exception as e:
        logging.error(f"Insights error for table {table_id}: {str(e)}")
        raise LVException("insight", f"Failed to compute insights for table {table_id}: {e}")


@app.get("/api/namespaces/{namespace}/insights")
def get_namespace_insights(
    namespace: str, 
    recursive: bool = Query(True, description="Search nested namespaces recursively?")
):
    runner = InsightsRunner(lv)
    try:
        results = runner.run_for_namespace(namespace, recursive=recursive)
        # results is a dict of {qualified_table_name: [Insight, ...]}
        return {k: [ins.__dict__ for ins in v] for k, v in results.items()}
    except Exception as e:
        logging.error(f"Insights error for namespace {namespace}: {str(e)}")
        raise LVException("insight", f"Failed to compute insights for namespace {namespace}: {e}")


@app.get("/api/lakehouse/insights")
def get_lakehouse_insights():
    runner = InsightsRunner(lv)
    try:
        results = runner.run_for_lakehouse()
        return {k: [ins.__dict__ for ins in v] for k, v in results.items()}
    except Exception as e:
        logging.error(f"Insights error for lakehouse: {str(e)}")
        raise LVException("insight", f"Failed to compute lakehouse insights: {e}")


def clean_cache():
    """Remove expired entries from the cache."""
    current_time = time.time()
    keys_to_delete = [
        key for key, (_, timestamp) in page_session_cache.items()
        if current_time - timestamp > CACHE_EXPIRATION
    ]
    for key in keys_to_delete:
        del page_session_cache[key]

# Schedule periodic cleanup every minute
def schedule_cleanup():
    clean_cache()
    Timer(60, schedule_cleanup).start()

# refresh namespaces and tables every 65 mins
def refresh_namespace_and_tables():
    read_tables(refresh=True, user='maintenance')    
    Timer(3900, refresh_namespace_and_tables).start()

schedule_cleanup()
refresh_namespace_and_tables()

