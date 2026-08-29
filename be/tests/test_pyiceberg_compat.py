"""Tests for the pyiceberg metadata compatibility shims."""

import pytest


def test_blob_metadata_accepts_engine_specific_types():
    """Trino/Presto puffin blob types must parse.

    pyiceberg declares BlobMetadata.type as a two-value Literal, but the Iceberg
    spec does not restrict it. A table carrying Trino's
    "presto-sum-data-size-bytes-v1" statistics failed validation, which made the
    entire table unloadable and surfaced in the UI as "Table not found" on every
    tab.
    """
    from app.pyiceberg_compat import apply_patches

    apply_patches()

    from pyiceberg.table.statistics import BlobMetadata

    for blob_type in (
        "presto-sum-data-size-bytes-v1",
        "presto-approx-distinct-values-v1",
        "apache-datasketches-theta-v1",  # still accepted
        "deletion-vector-v1",
        "some-future-engine-blob-v9",
    ):
        parsed = BlobMetadata.model_validate(
            {
                "type": blob_type,
                "snapshot-id": 1,
                "sequence-number": 1,
                "fields": [1],
            }
        )
        assert parsed.type == blob_type


def test_statistics_file_accepts_relaxed_blob_metadata():
    """StatisticsFile holds a list of BlobMetadata and must use the relaxed type."""
    from app.pyiceberg_compat import apply_patches

    apply_patches()

    from pyiceberg.table.statistics import StatisticsFile

    stats = StatisticsFile.model_validate(
        {
            "snapshot-id": 42,
            "statistics-path": "s3://bucket/stats.puffin",
            "file-size-in-bytes": 100,
            "file-footer-size-in-bytes": 10,
            "blob-metadata": [
                {
                    "type": "presto-sum-data-size-bytes-v1",
                    "snapshot-id": 42,
                    "sequence-number": 1,
                    "fields": [1],
                }
            ],
        }
    )
    assert stats.blob_metadata[0].type == "presto-sum-data-size-bytes-v1"


def test_apply_patches_is_idempotent():
    from app.pyiceberg_compat import apply_patches

    apply_patches()
    apply_patches()

    from pyiceberg.table.statistics import BlobMetadata

    assert BlobMetadata.model_fields["type"].annotation is str
