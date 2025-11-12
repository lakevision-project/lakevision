#!/usr/bin/env python3
"""
Utility helpers that seed intentionally “unhealthy” tables in the demo catalog.

These tables are used for demos and smoke-tests of the Lakevision Health feature.
They deliberately exercise every insight rule so the UI shows meaningful examples
right after `scripts/load_sample_data.py` finishes.
"""

from __future__ import annotations

import json
import logging
import math
import os
import random
import string
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

import numpy as np
import pyarrow as pa
from pyiceberg import catalog as catalog_module
from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import LongType, NestedField, StringType, UUIDType

logger = logging.getLogger(__name__)

DEFAULT_URI = os.getenv(
    "PYICEBERG_CATALOG__DEFAULT__URI",
    f"sqlite:///{Path('be/warehouse/sql-catalog.db').resolve()}",
)
DEFAULT_WAREHOUSE = os.getenv(
    "PYICEBERG_CATALOG__DEFAULT__WAREHOUSE",
    f"file://{Path('be/warehouse').resolve()}",
)
DEMO_NAMESPACE = os.getenv("LV_HEALTH_DEMO_NAMESPACE", "demo_health")
DYNAMIC_LARGE_FILE_BYTES = int(os.getenv("LV_DEMO_LARGE_FILE_BYTES", "500000000"))

def _load_catalog(uri: str | None = None, warehouse: str | None = None) -> Catalog:
    uri = uri or DEFAULT_URI
    warehouse = warehouse or DEFAULT_WAREHOUSE
    Path(warehouse.replace("file://", "")).mkdir(parents=True, exist_ok=True)
    logger.info("🔗 Using Iceberg catalog %s (warehouse=%s)", uri, warehouse)
    return catalog_module.load_catalog(
        "default",
        **{
            "type": "sql",
            "uri": uri,
            "warehouse": warehouse,
        },
    )


def _arrow_schema(fields: Sequence[tuple[str, pa.DataType, bool]]) -> pa.Schema:
    return pa.schema([pa.field(name, dtype, nullable=is_nullable) for name, dtype, is_nullable in fields])


def _random_string(length: int) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))


@dataclass
class DemoTableBuilder:
    catalog: Catalog
    namespace: str = DEMO_NAMESPACE

    def __post_init__(self) -> None:
        self.catalog.create_namespace_if_not_exists(self._namespace_tuple)

    @property
    def _namespace_tuple(self) -> tuple[str, ...]:
        return tuple(self.namespace.split("."))

    def _identifier(self, name: str) -> str:
        return f"{self.namespace}.{name}"

    def _create_table(
        self,
        name: str,
        schema: Schema,
        *,
        partition_spec: PartitionSpec | None = None,
        properties: dict[str, str] | None = None,
    ) -> Table:
        ident = self._identifier(name)
        if self.catalog.table_exists(ident):
            logger.info("🗑️  Dropping existing demo table %s", ident)
            self.catalog.drop_table(ident)
        logger.info("🆕 Creating demo table %s", ident)
        create_kwargs = {
            "identifier": ident,
            "schema": schema,
            "properties": properties or {},
        }
        if partition_spec is not None:
            create_kwargs["partition_spec"] = partition_spec
        return self.catalog.create_table(**create_kwargs)

    # ------------------------------------------------------------------ helpers

    def _append_many(self, table: Table, batches: Iterable[pa.Table]) -> None:
        for batch in batches:
            table.append(batch)

    # ------------------------------------------------------------------ builders

    def create_small_files_table(self, num_files: int = 150, rows_per_file: int = 5) -> None:
        """Many tiny files (SMALL_FILES rule)."""
        schema = Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "value", StringType()),
        )
        arrow_schema = _arrow_schema(
            [
                ("id", pa.int64(), False),
                ("value", pa.string(), True),
            ]
        )
        table = self._create_table("small_files", schema)

        def batches() -> Iterable[pa.Table]:
            for file_idx in range(num_files):
                start = file_idx * rows_per_file
                ids = list(range(start, start + rows_per_file))
                yield pa.table(
                    {
                        "id": ids,
                        "value": [f"demo-{file_idx}-{i}" for i in range(rows_per_file)],
                    },
                    schema=arrow_schema,
                )

        self._append_many(table, batches())
        logger.info("✅ Created %s with %s tiny files", table.name(), num_files)

    def create_uuid_empty_table(self) -> None:
        """Empty table with UUID column (UUID_COLUMN + NO_ROWS_TABLE)."""
        schema = Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "external_id", UUIDType()),
        )
        table = self._create_table("uuid_empty", schema)
        logger.info("✅ Created empty UUID table %s", table.name())

    def create_no_location_table(self) -> None:
        """Table with missing metadata location (NO_LOCATION)."""
        schema = Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "value", StringType()),
        )
        table = self._create_table("missing_location", schema)
        metadata_path = Path(table.metadata_location.replace("file://", ""))
        with metadata_path.open("r", encoding="utf-8") as src:
            metadata = json.load(src)
        metadata["location"] = ""
        tmp_path = metadata_path.with_suffix(".tmp")
        with tmp_path.open("w", encoding="utf-8") as dst:
            json.dump(metadata, dst)
        tmp_path.replace(metadata_path)
        logger.info("✅ Rewrote metadata for %s to remove location", table.name())

    def create_large_files_table(self, target_bytes: int | None = None) -> None:
        """Table with a single oversized file (LARGE_FILES rule)."""
        target_bytes = target_bytes or DYNAMIC_LARGE_FILE_BYTES
        schema = Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "value", StringType()),
            NestedField(3, "notes", StringType()),
        )
        arrow_schema = _arrow_schema(
            [
                ("id", pa.int64(), False),
                ("value", pa.string(), True),
                ("notes", pa.string(), True),
            ]
        )
        avg_row_size = 4200  # Rough bytes per row (due to notes payload)
        row_count = math.ceil(target_bytes / avg_row_size)
        payload = _random_string(4096)
        notes_column = [payload] * row_count
        table = self._create_table("large_files", schema)
        logger.info("🧱 Building large batch of ~%s rows for %s", row_count, table.name())
        batch = pa.table(
            {
                "id": list(range(row_count)),
                "value": [f"lf-{i % 1000}" for i in range(row_count)],
                "notes": notes_column,
            },
            schema=arrow_schema,
        )
        table.append(batch)
        logger.info("✅ Created %s with one oversized file (~%.2f GB)", table.name(), target_bytes / 1_000_000_000)

    def create_snapshot_sprawl_table(self, commits: int = 520) -> None:
        """Table with 500+ snapshots (SNAPSHOT_SPRAWL_TABLE)."""
        schema = Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "value", StringType()),
        )
        arrow_schema = _arrow_schema(
            [
                ("id", pa.int64(), False),
                ("value", pa.string(), True),
            ]
        )
        table = self._create_table("snapshot_sprawl", schema)
        for commit in range(commits):
            batch = pa.table(
                {
                    "id": [commit],
                    "value": [f"commit-{commit}"],
                },
                schema=arrow_schema,
            )
            table.append(batch)
        logger.info("✅ Created %s with %s snapshots", table.name(), commits)

    def create_skewed_partitions_table(self) -> None:
        """Partitioned table with a single hot partition (SKEWED_OR_LARGEST_PARTITIONS_TABLE)."""
        schema = Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "category", StringType()),
            NestedField(3, "value", LongType()),
        )
        spec = PartitionSpec(PartitionField(source_id=2, field_id=1000, transform="identity", name="category"))
        arrow_schema = _arrow_schema(
            [
                ("id", pa.int64(), False),
                ("category", pa.string(), True),
                ("value", pa.int64(), True),
            ]
        )
        table = self._create_table("skewed_partitions", schema, partition_spec=spec)
        heavy_rows = 200_000
        heavy_values = np.random.randint(0, 10_000, size=heavy_rows, dtype=np.int64).tolist()
        heavy_batch = pa.table(
            {
                "id": list(range(heavy_rows)),
                "category": ["critical"] * heavy_rows,
                "value": heavy_values,
            },
            schema=arrow_schema,
        )
        table.append(heavy_batch)
        light_categories = ("alpha", "beta", "gamma")
        for idx, cat_name in enumerate(light_categories, start=1):
            light_rows = 200
            light_values = np.random.randint(0, 1_000, size=light_rows, dtype=np.int64).tolist()
            light_batch = pa.table(
                {
                    "id": list(range(idx * 1_000_000, idx * 1_000_000 + light_rows)),
                    "category": [cat_name] * light_rows,
                    "value": light_values,
                },
                schema=arrow_schema,
            )
            table.append(light_batch)
        logger.info("✅ Created %s with heavily skewed partitions", table.name())

    def run(self) -> None:
        self.create_small_files_table()
        self.create_uuid_empty_table()
        self.create_no_location_table()
        self.create_large_files_table()
        self.create_snapshot_sprawl_table()
        self.create_skewed_partitions_table()


def seed_demo_health_tables(cat: Catalog | None = None, namespace: str = DEMO_NAMESPACE) -> None:
    builder = DemoTableBuilder(cat or _load_catalog(), namespace)
    builder.run()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    seed_demo_health_tables()


if __name__ == "__main__":
    main()
