import logging
import os
from urllib.parse import urlparse

import fsspec
import pyarrow.parquet as pq
from pyiceberg import catalog
from pyiceberg.io import FSSPEC_FILE_IO, PY_IO_IMPL

from create_test_iceberg_tables import seed_demo_health_tables

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# --- Read environment ---
uri = os.getenv("PYICEBERG_CATALOG__DEFAULT__URI")
warehouse_uri = os.getenv("PYICEBERG_CATALOG__DEFAULT__WAREHOUSE")

if not uri or not warehouse_uri:
    raise RuntimeError("Missing required environment variables: PYICEBERG_CATALOG__DEFAULT__URI and/or WAREHOUSE")

warehouse_path = urlparse(warehouse_uri).path
sqlite_path = urlparse(uri).path

sample_files = [
    {
        "url": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet",
        "table": "nyc_taxi",
        "description": "NYC Yellow Taxi Trips Jan 2023"
    },
    {
        "url": "https://github.com/RandomFractals/chicago-crimes/raw/refs/heads/main/data/crimes-2022.parquet?raw=true",
        "table": "chicago_crime",
        "description": "Sample of Chicago crime incidents"
    },
    {
        "url": "https://huggingface.co/datasets/princeton-nlp/SWE-bench_Verified/resolve/main/data/test-00000-of-00001.parquet?download=true",
        "table": "SWE_bench_Verified",
        "description": "SWE-bench Verified is a subset of 500 samples from the SWE-bench test set, which have been human-validated for quality",
    },
]


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


def download_data(url):
    logger.info(f"🌐 Downloading sample data from: {url}")
    fs = fsspec.filesystem("http")
    return pq.read_table(url, filesystem=fs)


def create_table_if_missing(cat, table_name, url):
    if cat.table_exists(table_name):
        logger.info(f"📄 Table already exists: {table_name}")
        return

    logger.info(f"🛠️ Creating new table: {table_name}")
    table = download_data(url)
    cat.create_table(identifier=table_name, schema=table.schema)
    cat.load_table(table_name).append(table)
    logger.info(f"✅ Loaded {table.num_rows} rows into {table_name}")


def main():
    ensure_warehouse()
    cat = load_catalog()
    cat.create_namespace_if_not_exists("default")
    for sample in sample_files:
        table_name = f'default.{sample["table"]}'
        create_table_if_missing(cat, table_name, sample["url"])
    if os.getenv("LV_SKIP_HEALTH_DEMO", "false").lower() not in {"1", "true", "yes"}:
        logger.info("🧪 Generating demo health tables...")
        seed_demo_health_tables(cat)
    else:
        logger.info("⏭️  Skipping demo health tables (LV_SKIP_HEALTH_DEMO was set)")

if __name__ == "__main__":
    main()
