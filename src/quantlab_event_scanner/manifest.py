"""Schema-tolerant manifest parsing helpers."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Mapping


_PARTITION_PATH_RE = re.compile(
    r"(?:^|/)exchange=(?P<exchange>[^/]+)/stream=(?P<stream>[^/]+)/"
    r"symbol=(?P<symbol>[^/]+)/date=(?P<date>[^/]+)(?:/|$)"
)


@dataclass(frozen=True)
class ManifestPartition:
    """Best-effort partition metadata extracted from a manifest."""

    exchange: str | None = None
    stream: str | None = None
    symbol: str | None = None
    date: str | None = None
    path: str | None = None


@dataclass(frozen=True)
class ManifestMetadata:
    """Parsed manifest metadata that preserves the original object."""

    raw: Mapping[str, Any]
    partitions: tuple[ManifestPartition, ...] = field(default_factory=tuple)


def parse_manifest_json(obj: Mapping[str, Any]) -> ManifestMetadata:
    """Parse a manifest object without assuming a rigid schema."""

    if not isinstance(obj, Mapping):
        raise TypeError("Manifest must be a mapping.")

    partitions: list[ManifestPartition] = []
    seen: set[tuple[str | None, str | None, str | None, str | None, str | None]] = set()

    def add_partition(partition: ManifestPartition) -> None:
        key = (
            partition.exchange,
            partition.stream,
            partition.symbol,
            partition.date,
            partition.path,
        )
        if key not in seen:
            seen.add(key)
            partitions.append(partition)

    def visit(value: Any) -> None:
        if isinstance(value, Mapping):
            mapped_partition = _partition_from_mapping(value)
            if mapped_partition is not None:
                add_partition(mapped_partition)

            for nested in value.values():
                visit(nested)
            return

        if isinstance(value, list):
            for item in value:
                visit(item)
            return

        if isinstance(value, str):
            path_partition = _partition_from_path(value)
            if path_partition is not None:
                add_partition(path_partition)

    visit(obj)
    return ManifestMetadata(raw=obj, partitions=tuple(partitions))


def load_manifest_from_s3_with_spark(spark: Any, manifest_path: str) -> ManifestMetadata:
    """Future Databricks/Spark manifest loader placeholder."""

    raise NotImplementedError(
        "Manifest loading from S3 with Spark is reserved for a future Databricks phase."
    )


def _partition_from_mapping(value: Mapping[str, Any]) -> ManifestPartition | None:
    exchange = _optional_string(value.get("exchange"))
    stream = _optional_string(value.get("stream"))
    symbol = _optional_string(value.get("symbol"))
    date = _optional_string(value.get("date"))
    path = _first_path_string(value)

    if not any((exchange, stream, symbol, date)):
        return None

    return ManifestPartition(
        exchange=exchange,
        stream=stream,
        symbol=symbol,
        date=date,
        path=path,
    )


def _partition_from_path(value: str) -> ManifestPartition | None:
    match = _PARTITION_PATH_RE.search(value)
    if match is None:
        return None

    return ManifestPartition(
        exchange=match.group("exchange"),
        stream=match.group("stream"),
        symbol=match.group("symbol"),
        date=match.group("date"),
        path=value,
    )


def _first_path_string(value: Mapping[str, Any]) -> str | None:
    for key in ("path", "s3_path", "uri", "file"):
        candidate = value.get(key)
        if isinstance(candidate, str):
            return candidate
    return None


def _optional_string(value: Any) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None
