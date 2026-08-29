from app.pyiceberg_compat import apply_patches

# Must run before any catalog or table model is constructed.
apply_patches()

from pyiceberg import catalog
from pyiceberg.catalog import Identifier
from pyiceberg.expressions import AlwaysTrue
from typing import List, Union
import json, os, time, re
import pandas as pd
import pyarrow as pa
import daft
from daft.context import get_context
import humanize
import pyarrow.compute as pc
import numpy as np
import google.auth
from google.auth.transport.requests import Request
from app.sql_guard import validate_and_bind
import logging

class LakeView():
    
    def __init__(self):        
        service_account_file = os.environ.get("GCP_KEYFILE")
        if service_account_file and service_account_file != "":
            scopes = ["https://www.googleapis.com/auth/cloud-platform"]
            access_token = get_gcp_access_token(service_account_file, scopes)                        
            self.catalog = catalog.load_catalog("default", 
                **{
                    "gcs.oauth2.token-expires-at": time.mktime(access_token.expiry.timetuple()) * 1000,
                    "gcs.oauth2.token": access_token.token,        
                })
        else:
            self.catalog = catalog.load_catalog("default")        
        self.namespace_options = []
        # Keyed by (table name, snapshot id); see get_file_data.
        self._files_cache = {}

    def get_namespaces(_self, include_nested: bool = True):
        result = []
        namespaces = _self.catalog.list_namespaces()
        for ns in namespaces:
            new_ns = ns if len(ns) == 1 else ns[:1]
            result.append(new_ns)
            if (include_nested):
                result += _self._get_nested_namespaces(new_ns, 1)
        result = list(result)
        result.sort()
        return result

    def _get_nested_namespaces(self, namespace: Union[str, Identifier] = (), level: int = 1) -> List[Identifier]:
        result = []
        namespaces = self.catalog.list_namespaces(namespace)
        for ns in namespaces:
            #pyiceberg includes the initial level at the beginning for nested namespaces
            fixed_ns = ns if (len(ns) == (level + 1)) else ns[level:]
            result.append(fixed_ns)
            result += self._get_nested_namespaces(fixed_ns, level + 1)
        return result
    
    def get_tables(self, namespace: str):
        tables = self.catalog.list_tables(namespace)
        tables.sort()
        return tables
    
    def get_all_table_names(self, namespaces: List[str]):
        all_tables = {}
        for namespace in namespaces:
            tabs = self.catalog.list_tables(namespace)            
            ns_tab = []
            for tab in tabs:
                ns_tab.append(tab[-1])
                ns_tab.sort()
            all_tables[namespace] = ns_tab            
        return all_tables

    def load_table(self, table_id: str):
        table = self.catalog.load_table(table_id)
        return table
    
    def get_partition_data(self, table):        
        #table = self.catalog.load_table(table_id)
        if not table.metadata.current_snapshot_id:
            return pd.DataFrame()
        pa_partitions = table.inspect.partitions()        
        if pa_partitions.num_rows >1:
            pa_partitions = pa_partitions.sort_by([('partition', 'ascending')])
        return pa_partitions.to_pandas()

    def get_snapshot_data(self, table):        
        if not table.metadata.current_snapshot_id:
            return pd.DataFrame()
        pa_snaps = table.inspect.snapshots().sort_by([('committed_at', 'descending')])
        df = pa_snaps.to_pandas()
        df['committed_at'] = df['committed_at'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        df['id'] = df.index
        return df
    
    # Manifest entry content codes, per the Iceberg spec.
    _FILE_CONTENT = {0: "Data", 1: "Position deletes", 2: "Equality deletes"}

    def get_file_data(self, table, offset: int = 0, limit: int = 100):
        """Return a page of the table's data/delete files, newest spec first.

        `inspect.files()` returns one row per file with full column statistics --
        lower/upper bounds as raw binary maps and a deeply nested
        `readable_metrics` struct. Serialising all of that for a large table is
        not viable: a 15,395-file table produces ~300 MB of JSON and takes
        several seconds. So this selects the columns that are useful in a listing
        and pages server-side.

        Returns (records, total_count).
        """
        if not table.metadata.current_snapshot_id:
            return [], 0

        # inspect.files() reads every manifest, which is the expensive part and is
        # identical for every page of the same snapshot. Cache the arrow table per
        # (table, snapshot) so paging does not pay that cost repeatedly.
        cache_key = (table.name(), table.metadata.current_snapshot_id)
        pa_files = self._files_cache.get(cache_key)
        if pa_files is None:
            pa_files = table.inspect.files()
            # Small bound: these are big objects and users page one table at a time.
            if len(self._files_cache) >= 4:
                self._files_cache.pop(next(iter(self._files_cache)))
            self._files_cache[cache_key] = pa_files
        total = pa_files.num_rows
        if total == 0:
            return [], 0

        offset = max(0, int(offset))
        limit = max(1, min(int(limit), 1000))
        if offset >= total:
            return [], total

        # Slice before converting, so only the requested page is materialised.
        page = pa_files.slice(offset, limit)

        keep = [
            name
            for name in (
                "content",
                "file_path",
                "file_format",
                "spec_id",
                "partition",
                "record_count",
                "file_size_in_bytes",
                "sort_order_id",
            )
            if name in page.schema.names
        ]
        df = page.select(keep).to_pandas()

        if "content" in df.columns:
            df["content"] = df["content"].map(
                lambda c: self._FILE_CONTENT.get(int(c), f"Unknown ({c})")
                if pd.notna(c)
                else ""
            )
        if "file_format" in df.columns:
            df["file_format"] = df["file_format"].astype(str)
        if "partition" in df.columns:
            # A struct renders as an unreadable dict; flatten to "key=value".
            df["partition"] = df["partition"].map(self._format_partition)

        rename = {
            "content": "Content",
            "file_path": "File path",
            "file_format": "Format",
            "spec_id": "Spec",
            "partition": "Partition",
            "record_count": "Records",
            "file_size_in_bytes": "Size (bytes)",
            "sort_order_id": "Sort order",
        }
        df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
        # Stable row identity for the UI's keyed each-blocks.
        df["id"] = range(offset, offset + len(df))
        return df, total

    @staticmethod
    def _format_partition(value):
        """Render a partition struct as "col=value", or "" when unpartitioned."""
        if not value:
            return ""
        if isinstance(value, dict):
            return ", ".join(f"{k}={v}" for k, v in value.items() if v is not None)
        return str(value)

    def get_data_change(self, table):        
        #table = self.catalog.load_table(table_id)
        pa_snaps = table.inspect.snapshots().sort_by([('committed_at', 'ascending')])
        pa_snaps = pa_snaps.drop(['snapshot_id', 'parent_id', 'operation', 'manifest_list'])
        df = pa_snaps.to_pandas()
        df['committed_at'] = df['committed_at'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))              
        df_summ = pd.DataFrame(df['summary'].apply(self.flatten_tuples).tolist())
        df_flattened = pd.concat([df.drop('summary', axis=1), df_summ], axis=1)        
        return df_flattened                

    def get_sample_data(self, table, sql, limit=50):
        df = daft.read_iceberg(table)         
        if sql:
            namespace = table.catalog.namespace_to_string(table.catalog.namespace_from(table.name()))
            if 'default.' in namespace:
                namespace = namespace.replace('default.', '')
            table_name = table.catalog.table_name_from(table.name())
            # Validate before execution and bind the table reference to the `df`
            # alias registered above. Daft's SQL dialect can read arbitrary paths
            # via read_parquet/read_csv, so this is an authorization boundary:
            # the query must not be able to name anything but this table.
            sql = validate_and_bind(sql, f"{namespace}.{table_name}")
            logging.info("Executing validated SQL: %s", sql)
            df = daft.sql(sql)
            curr_snapshot = table.current_snapshot()
            if (
                curr_snapshot
                and "total-data-files" in curr_snapshot.summary.keys()
                and int(curr_snapshot.summary["total-data-files"]) > 200
            ):
                optimized_plan = df._builder.optimize(get_context().daft_execution_config)._builder.repr_ascii(
                    simple=False
                )
                logging.info(optimized_plan)
                num_tasks = int(self.extract_num_scan_tasks(optimized_plan))
                logging.info(f"Num tasks {num_tasks}")
                if num_tasks > 300:
                    raise Exception(
                        f"Number of scan tasks ({num_tasks}) too high. Optimize the query or use a distributed query tool."
                    )            
        else:        
            df = df.limit(limit)
        paT = df.to_arrow()
        paT = self.convertTimestamp(paT)
        return paT.to_pandas()
       

    def get_schema(self, table):
        #table = self.catalog.load_table(table_id)
        df = pd.DataFrame(columns=["Field_id", "Field", "DataType", "Required", "Comments"])
        for field in table.schema().fields:
            df2 = pd.DataFrame([[str(field.field_id), str(field.name), str(field.field_type), str(field.required), field.doc]], columns=["Field_id", "Field", "DataType", "Required", "Comments"])
            df = pd.concat([df, df2])
        return df
    
    def get_summary(self, table):
        #table = self.catalog.load_table(table_id)
        ret = {}         
        ret['Location'] = table.location()
        ret['Current snapshotid'] = table.metadata.current_snapshot_id
        if table.metadata.current_snapshot_id:
            paTable = table.inspect.snapshots().sort_by([('committed_at', 'descending')]).select(['summary', 'committed_at'])
            ret['Last updated (UTC)'] = paTable.to_pydict()['committed_at'][0].strftime('%Y-%m-%d %H:%M:%S')            
            result = dict(paTable.to_pydict()['summary'][0])
            total_records = int(result.get("total-records", -1))
            total_file_size = int(result.get("total-files-size", -1))
            total_data_files = int(result.get("total-data-files", -1))            
            # snapshot summary doesn't always contains following 3 properties hence getting from files meta, which is slower
            if total_records == -1 or total_file_size == -1 or total_data_files == -1:
                files_meta = table.inspect.files().select(['record_count', 'file_size_in_bytes'])
                total_records = pc.sum(files_meta['record_count']).as_py()
                total_file_size = pc.sum(files_meta['file_size_in_bytes']).as_py()
                total_data_files = files_meta.num_rows
            ret['Total records'] = humanize.intcomma(total_records)
            ret['Total file size'] = humanize.naturalsize(total_file_size)
            ret['Total data files'] = humanize.intcomma(total_data_files)

            ret['Total delete files'] = result.get('total-delete-files', 0)        
            ret['Total snapshots'] = paTable.num_rows 
        else:
            ret['Total records'] = '0'
        ret['Format version'] = table.metadata.format_version
        ret['Identifier fields'] = ''
        if len(table.schema().identifier_field_names()) > 0:
                ret['Identifier fields'] = list(table.schema().identifier_field_names())
        return ret

    def get_properties(self, table):
        #table = self.catalog.load_table(table_id)
        return table.properties
        
    def get_partition_specs(self, table):
        #table = self.catalog.load_table(table_id)
        partitionfields=table.spec().fields
        result = []
        for f in partitionfields:
            result.append({
                "Field": table.schema().find_column_name(f.source_id), 
                "Name": f.name, 
                "Transform": str(f.transform)
                })
        return result
    
    def get_sort_order(self, table):
        sorts = []
        for fld in table.sort_order().fields:    
            ret = {}
            ret["Field"] = table.schema().find_column_name(fld.source_id)
            ret["Transform"] = str(fld.transform)
            ret["Direction"] = fld.direction.name
            ret["Null Order"] = fld.null_order.name
            sorts.append(ret)
        return sorts

    def get_row_filter(self, partition, table):
        if partition is None or len(partition) == 0:
            return AlwaysTrue()
        fields = table.spec().fields
        use_fields = []
        for field in fields:
            source_field = table.schema().find_column_name(field.source_id)            
            if 'bucket' in str(field.transform):
                continue  #filter by bucket not yet supported, add others not supported too
            use_fields.append(source_field)
        expression=''
        idx = 0
        for key, value in partition.items():
            if key in use_fields:
                if idx == 0 or len(use_fields)==1:
                    expression = f"{key}=='{value}'"
                else:
                    expression += f" and {key}=='{value}'"
            idx += 1
        return expression if len(expression) > 0 else AlwaysTrue()
        
    # Flattening the tuple array into separate columns
    def flatten_tuples(self, row):    
        return {k: v for k, v in row}
    
    def convertTimestamp(self, paT: pa.Table):
        for col in paT.schema.names:
            if isinstance(paT.schema.field(col).type, pa.TimestampType):
                paT = paT.set_column(
                    paT.schema.get_field_index(col),
                    col,
                    pc.strftime(paT[col], format="%Y-%m-%d %H:%M:%S")
                )
        return paT
    
    def extract_num_scan_tasks(self, log_output):
        match = re.search(r'\* Num Scan Tasks\s*=\s*(\d+)', log_output)
        if match:
            return int(match.group(1))
        return None
        
def get_gcp_access_token(service_account_file, scopes):
    """
    Retrieves an access token from Google Cloud Platform using service account credentials.
    Args:
        service_account_file: Path to the service account JSON key file.
        scopes: List of OAuth scopes required for your application.
    Returns:
        The access token as a string.
    """
    credentials, name = google.auth.load_credentials_from_file(
        service_account_file, scopes=scopes)

    request = Request()
    credentials.refresh(request)  # Forces token refresh if needed
    return credentials
