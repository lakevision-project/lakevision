import logging
import os

import fsspec
import pyarrow.parquet as pq
from pyiceberg import catalog
from pyiceberg.io import PY_IO_IMPL, FSSPEC_FILE_IO
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# --- Read environment ---
uri = os.getenv("PYICEBERG_CATALOG__DEFAULT__URI")
warehouse_uri = os.getenv("PYICEBERG_CATALOG__DEFAULT__WAREHOUSE")

if not uri or not warehouse_uri:
    raise RuntimeError("Missing required environment variables: PYICEBERG_CATALOG__DEFAULT__URI and/or WAREHOUSE")

warehouse_path = urlparse(warehouse_uri).path
sqlite_path = urlparse(uri).path

TABLE_NAME = "default.taxi_dataset"
DATA_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"

def ensure_warehouse():
    os.makedirs(warehouse_path, exist_ok=True)
    logger.info(f"✅ Warehouse directory ensured at: {warehouse_path}")


def load_catalog():
    logger.info("🔗 Initializing Iceberg catalog using SQLite backend...")
    return catalog.load_catalog(
        "default",
        **{
            "type": "sql",
            "uri": f"sqlite:///{sqlite_path}",
            "warehouse": f"file://{warehouse_path}",
            PY_IO_IMPL: FSSPEC_FILE_IO,
        },
    )


def download_data():
    logger.info(f"🌐 Downloading sample data from: {DATA_URL}")
    fs = fsspec.filesystem("http")
    return pq.read_table(DATA_URL, filesystem=fs)


def create_table_if_missing(cat):
    if cat.table_exists(TABLE_NAME):
        logger.info(f"📄 Table already exists: {TABLE_NAME}")
        return

    logger.info(f"🛠️ Creating new table: {TABLE_NAME}")
    table = download_data()
    cat.create_table(identifier=TABLE_NAME, schema=table.schema)
    cat.load_table(TABLE_NAME).append(table)
    logger.info(f"✅ Loaded {table.num_rows} rows into {TABLE_NAME}")


def main():
    ensure_warehouse()
    cat = load_catalog()
    cat.create_namespace_if_not_exists("default")
    create_table_if_missing(cat)


if __name__ == "__main__":
    main()
