"""Compatibility shims for pyiceberg metadata parsing.

Imported for its side effects, before any catalog is created. Each patch below
works around an upstream model being stricter than the Iceberg spec, which makes
otherwise-valid tables unloadable.
"""

import logging

logger = logging.getLogger(__name__)


def _relax_blob_metadata_type() -> None:
    """Accept any puffin blob type, not just the two pyiceberg hardcodes.

    ``BlobMetadata.type`` is declared as
    ``Literal["apache-datasketches-theta-v1", "deletion-vector-v1"]``, but the
    Iceberg spec does not restrict the blob type: engines register their own. A
    table written by Trino/Presto carries ``presto-sum-data-size-bytes-v1`` and
    similar, so pydantic rejects the whole TableResponse and the table cannot be
    loaded at all -- surfacing to users as "Table not found" on every tab.

    The type is descriptive metadata that Lakevision only reads, so widening it to
    ``str`` loses nothing. Still present on pyiceberg main as of 0.11.1.
    """
    try:
        from pydantic import Field

        from pyiceberg.table import statistics as statistics_module
        from pyiceberg.typedef import IcebergBaseModel
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Could not apply BlobMetadata compatibility patch: %s", exc)
        return

    existing = getattr(statistics_module, "BlobMetadata", None)
    if existing is None:
        return

    annotation = existing.model_fields.get("type").annotation if existing.model_fields.get("type") else None
    if annotation is str:
        return  # already permissive upstream; nothing to do

    class BlobMetadata(IcebergBaseModel):
        # Deliberately `str` rather than a Literal -- see the docstring.
        type: str
        snapshot_id: int = Field(alias="snapshot-id")
        sequence_number: int = Field(alias="sequence-number")
        fields: list[int]
        properties: dict[str, str] | None = None

    statistics_module.BlobMetadata = BlobMetadata

    # StatisticsFile holds a list[BlobMetadata]; repoint it and rebuild so models
    # constructed later (TableResponse among them) pick up the relaxed type.
    statistics_file = getattr(statistics_module, "StatisticsFile", None)
    if statistics_file is not None:
        field = statistics_file.model_fields.get("blob_metadata")
        if field is not None:
            field.annotation = list[BlobMetadata]
            statistics_file.model_rebuild(force=True)

    logger.info(
        "Relaxed pyiceberg BlobMetadata.type to accept engine-specific puffin "
        "blob types (e.g. Trino's presto-sum-data-size-bytes-v1)."
    )


def apply_patches() -> None:
    """Apply every compatibility patch. Safe to call more than once."""
    _relax_blob_metadata_type()
