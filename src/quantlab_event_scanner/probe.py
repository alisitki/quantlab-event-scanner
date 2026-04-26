"""Pure helpers for Databricks probe jobs."""

from __future__ import annotations

from .manifest import ManifestMetadata, ManifestPartition


def latest_partition_date(manifest: ManifestMetadata) -> str:
    """Return the max available partition date from a parsed manifest."""

    dates = [partition.date for partition in manifest.partitions if partition.date]
    if not dates:
        raise ValueError("Manifest does not contain any partition dates.")
    return max(dates)


def select_btc_trade_partitions(
    manifest: ManifestMetadata,
    date: str | None = None,
) -> tuple[ManifestPartition, ...]:
    """Select latest-date trade partitions whose symbol contains btc."""

    selected_date = date or latest_partition_date(manifest)
    return tuple(
        partition
        for partition in manifest.partitions
        if partition.date == selected_date
        and partition.available is not False
        and partition.stream == "trade"
        and partition.symbol is not None
        and "btc" in partition.symbol.lower()
    )


def partition_paths(
    partitions: tuple[ManifestPartition, ...],
    input_root: str | None = None,
) -> tuple[str, ...]:
    """Return parquet read paths from manifest artifact data keys."""

    paths = tuple(_read_path(partition, input_root) for partition in partitions if partition.path)
    if len(paths) != len(partitions):
        raise ValueError("Selected manifest partitions must all include artifact data paths.")
    if not paths:
        raise ValueError("No matching manifest partition paths found.")
    return paths


def _read_path(partition: ManifestPartition, input_root: str | None) -> str:
    path = partition.path
    if path is None:
        raise ValueError("Selected manifest partition must include an artifact data path.")
    if path.startswith("s3://") or input_root is None:
        return path
    return f"{input_root.rstrip('/')}/{path.lstrip('/')}"
