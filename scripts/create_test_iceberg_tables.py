import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog, Catalog
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, LongType, NestedField
from pyiceberg.table import Table
from pyiceberg.types import StringType, LongType
from pyiceberg.partitioning import PartitionSpec

CATALOG_PATH = os.path.abspath("./be/warehouse-test")
os.makedirs(CATALOG_PATH, exist_ok=True)

CATALOG_DB = os.path.join(CATALOG_PATH, "testcat.db")

def create_catalog():
    return load_catalog(
        "default",  # Or whatever is set in your .env as the catalog name
        type="sql",
        uri=f"sqlite:///{CATALOG_DB}",
        warehouse=f"file://{CATALOG_PATH}"
    )

def write_parquet_files(table_dir, n_files=120, rows_per_file=10):
    os.makedirs(table_dir, exist_ok=True)
    file_paths = []
    arrow_schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),    # Required!
        pa.field("value", pa.string(), nullable=True),
    ])
    for i in range(n_files):
        df = pd.DataFrame({
            "id": [i * rows_per_file + j for j in range(rows_per_file)],
            "value": [f"val_{i}_{j}" for j in range(rows_per_file)],
        })
        df["id"] = df["id"].astype("int64")
        table = pa.Table.from_pandas(df, schema=arrow_schema, preserve_index=False)
        file_path = os.path.join(table_dir, f"part-{i:03d}.parquet")
        pq.write_table(table, file_path)
        file_paths.append(file_path)
    return file_paths


def create_small_files_table(catalog: Catalog, namespace: str, table_name: str):
    table_id = f"{namespace}.{table_name}"
    # Remove existing table if exists
    if catalog.table_exists(table_id):
        catalog.drop_table(table_id)
    
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "value", StringType(), required=False)
    )
    catalog.create_namespace_if_not_exists(namespace)
    table = catalog.create_table(
        identifier=table_id,
        schema=schema
    )

    # Use canonical Iceberg location for data files
    table_dir = table.location().replace("file://", "")
    file_paths = write_parquet_files(table_dir, n_files=120, rows_per_file=5)

    # Register all files with Iceberg metadata!
    table.add_files(file_paths)

    print(f"Created SMALL_FILES table at {table_dir} with {len(file_paths)} small files.")


def create_no_location_table(catalog: Catalog, namespace: str, table_name: str):
    # This is mostly theoretical since Iceberg tables almost always have a location
    # We'll simulate by creating a table, then manually removing its location field
    table_id = f"{namespace}.{table_name}"
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "value", StringType(), required=False)
    )
    catalog.create_namespace_if_not_exists(namespace)
    if catalog.table_exists(table_id):
        catalog.drop_table(table_id)
    t = catalog.create_table(
        identifier=table_id,
        schema=schema,
        partition_spec=PartitionSpec()
    )
    # TODO mmm how to manually set location to None? Can't find a way to break it yet
    print(f"Created NO_LOCATION table '{table_id}' (simulate location=None in tests).")

if __name__ == "__main__":
    catalog = create_catalog()
    create_small_files_table(catalog, "testns", "smallfiles")
    create_no_location_table(catalog, "testns", "noloc")
    print("\nDone! Use these tables for local rule validation/testing.\n")
