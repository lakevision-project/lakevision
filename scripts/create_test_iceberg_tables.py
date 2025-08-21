import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog, Catalog
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, LongType, NestedField
from pyiceberg.table import Table
from pyiceberg.types import StringType, LongType, UUIDType
from pyiceberg.partitioning import PartitionSpec
import numpy as np
import random
import string

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

# Create a fixed-length random string to make size estimation easier
def generate_random_string(length):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def write_large_parquet_files(table_dir, n_files=1):
    TARGET_SIZE_BYTES = 1.5 * 1024**3
    os.makedirs(table_dir, exist_ok=True)
    file_paths = []
    arrow_schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),    # Required!
        pa.field("value", pa.string(), nullable=True),
        pa.field("notes", pa.string(), nullable=True),
    ])


    for i in range(n_files):
        data = {
        'id': [],
        'value': [],
        'notes': []
        }
        current_rows = 0

        # Incrementally add rows until the DataFrame's in-memory size is large enough
        while True:
            new_rows = 500000  # Add rows in batches
            
            data['id'].extend([current_rows + j for j in range(new_rows)])
            data['value'].extend([str(val) for val in np.random.randint(0, 1000, new_rows)])
            data['notes'].extend([generate_random_string(200) for _ in range(new_rows)])

            current_rows += new_rows
            
            # Create the DataFrame from the collected data
            df = pd.DataFrame(data)
            
            # Estimate the in-memory size and check if we've reached our target
            in_memory_size = df.memory_usage(deep=True).sum()
            print(f"Current rows: {current_rows:,} | In-memory size: {in_memory_size / 1024**2:.2f} MB")
            
            if in_memory_size >= TARGET_SIZE_BYTES:
                break

        print(f"\nDataFrame created with {df.shape[0]:,} rows.")
        
        df["id"] = df["id"].astype("int64")
        table = pa.Table.from_pandas(df, schema=arrow_schema, preserve_index=False)
        file_path = os.path.join(table_dir, f"part-{i:03d}.parquet")
        pq.write_table(table, file_path)
        file_paths.append(file_path)
    return file_paths

def create_large_files_table(catalog: Catalog, namespace: str, table_name: str):
    table_id = f"{namespace}.{table_name}"
    # Remove existing table if exists
    if catalog.table_exists(table_id):
        catalog.drop_table(table_id)
    
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "value", StringType(), required=False),
        NestedField(3, "notes", StringType(), required=False),
    )
    catalog.create_namespace_if_not_exists(namespace)
    table = catalog.create_table(
        identifier=table_id,
        schema=schema
    )

    # Use canonical Iceberg location for data files
    table_dir = table.location().replace("file://", "")
    file_paths = write_large_parquet_files(table_dir, n_files=1)

    # Register all files with Iceberg metadata!
    table.add_files(file_paths)

    print(f"Created LARGE_FILES table at {table_dir} with {len(file_paths)} large files.")

def create_uuid_column_and_empty_table(catalog: Catalog, namespace: str, table_name: str):
    table_id = f"{namespace}.{table_name}"
    # Remove existing table if exists
    if catalog.table_exists(table_id):
        catalog.drop_table(table_id)
    
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "value", UUIDType(), required=False)
    )
    catalog.create_namespace_if_not_exists(namespace)
    table = catalog.create_table(
        identifier=table_id,
        schema=schema
    )

    # Use canonical Iceberg location for data files
    table_dir = table.location().replace("file://", "")
    file_paths =[]

    print(f"Created table with uuid column at {table_dir} with {len(file_paths)} files.")    

if __name__ == "__main__":
    catalog = create_catalog()
    create_small_files_table(catalog, "testns", "smallfiles")
    create_no_location_table(catalog, "testns", "noloc")
    create_large_files_table(catalog, "testns", "largefiles")
    create_uuid_column_and_empty_table(catalog, "testns", "uuidcolumn")
    print("\nDone! Use these tables for local rule validation/testing.\n")
