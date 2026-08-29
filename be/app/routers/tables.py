from fastapi import APIRouter, Depends, Request, Response, status, HTTPException
from pyiceberg.table import Table

from app import config 
from app.dependencies import get_table, lv, authz_, check_auth
from app.api_utils import df_to_records
from app.exceptions import LVException
from app.sql_guard import SQLValidationError
import logging

router = APIRouter()

@router.get("/api/namespaces")
def read_namespaces(refresh: bool = False):
    from app.dependencies import namespaces # Use global
    if refresh or not namespaces:
        namespaces[:] = lv.get_namespaces() # Modify in place
    return [{"id": idx, "text": ".".join(ns)} for idx, ns in enumerate(namespaces)]

@router.get("/api/tables")
def read_tables(namespace: str = None, refresh: bool = False, user=Depends(check_auth)):
    # THIS LINE WAS MISSING AND SHOULD BE ADDED BACK
    if config.AUTH_ENABLED and not user:
        raise HTTPException(status_code=401, detail="User not logged in")
    ret = []
    from app.dependencies import namespaces, ns_tables
    if not namespace:
        if refresh or not ns_tables:
            if not namespaces:
                namespaces[:] = lv.get_namespaces()
            ns_tables.clear()
            ns_tables.update(lv.get_all_table_names(namespaces))
        for namespace, tables in ns_tables.items():           
            for idx, table in enumerate(tables):            
                ret.append({"id": idx, "text": table, "namespace": ".".join(namespace)})
        return ret
    for idx, table in enumerate(lv.get_tables(namespace)):
        ret.append({"id": idx, "text": table[-1], "namespace": namespace})        
    return ret


@router.get("/api/namespaces/{namespace}/special-properties")
def read_namespaces_special_properties(namespace: str):
    return authz_.get_namespace_special_properties(namespace)

@router.get("/api/tables/{table_id}/special-properties")
def read_table_special_properties(table_id: str):
    return authz_.get_table_special_properties(table_id)

@router.get("/api/tables/{table_id}/snapshots")
def read_table_snapshots(table: Table = Depends(get_table)):
    return df_to_records(lv.get_snapshot_data(table))

@router.get("/api/tables/{table_id}/partitions", status_code=status.HTTP_200_OK)
def read_table_partitions(request: Request, response: Response, table_id: str, table: Table = Depends(get_table)):
    if not authz_.has_access(request, response, table_id):
        return
    return df_to_records(lv.get_partition_data(table))

@router.get("/api/tables/{table_id}/sample", status_code=status.HTTP_200_OK)
def read_sample_data(request: Request, response: Response, table_id: str, sql: str = None, sample_limit: int = 100, table: Table = Depends(get_table)):
    if not authz_.has_access(request, response, table_id):
        return
    try:
        res = lv.get_sample_data(table, sql, sample_limit)
        return df_to_records(res)
    except SQLValidationError as e:
        # User-facing and safe to echo: the validator's messages describe only the
        # query the caller supplied.
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # Engine/catalog errors can embed storage paths and credential hints, so
        # log the detail and return something generic.
        logging.exception("Sample data query failed for %s", table_id)
        raise LVException("err", "Query could not be executed. Check the server logs for details.")

# ... Add the remaining table endpoints here ...
# (/schema, /summary, /properties, /partition-specs, /sort-order, /data-change)

@router.get("/api/tables/{table_id}/schema")
def read_schema_data(table: Table = Depends(get_table)):
    return df_to_records(lv.get_schema(table))

@router.get("/api/tables/{table_id}/summary")
def read_summary_data(table: Table = Depends(get_table)):
    return lv.get_summary(table)

@router.get("/api/tables/{table_id}/properties")
def read_properties_data(table: Table = Depends(get_table)):
    return lv.get_properties(table)

@router.get("/api/tables/{table_id}/partition-specs")
def read_partition_specs(table: Table = Depends(get_table)):
    return lv.get_partition_specs(table)

@router.get("/api/tables/{table_id}/sort-order")
def read_sort_order(table: Table = Depends(get_table)):
    return lv.get_sort_order(table)

@router.get("/api/tables/{table_id}/files", status_code=status.HTTP_200_OK)
def read_table_files(
    request: Request,
    response: Response,
    table_id: str,
    offset: int = 0,
    limit: int = 100,
    table: Table = Depends(get_table),
):
    """A page of the table's data and delete files.

    Paged server-side: the underlying inspect.files() carries full column
    statistics per file, so a large table would otherwise produce hundreds of
    megabytes of JSON. Gated by the same authz check as the other data-bearing
    endpoints, since file paths and per-file row counts describe the data itself.
    """
    if not authz_.has_access(request, response, table_id):
        return
    df, total = lv.get_file_data(table, offset=offset, limit=limit)
    records = df_to_records(df) if len(df) else []
    return {"items": records, "total": total, "offset": offset, "limit": limit}

@router.get("/api/tables/{table_id}/data-change")
def read_data_change(table: Table = Depends(get_table)):
    return df_to_records(lv.get_data_change(table))