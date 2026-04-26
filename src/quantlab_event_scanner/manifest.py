"""Schema-tolerant manifest parsing helpers."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from json import JSONDecodeError
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
    available: bool | None = None
    artifacts: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ManifestMetadata:
    """Parsed manifest metadata that preserves the original object."""

    raw: Mapping[str, Any]
    partitions: tuple[ManifestPartition, ...] = field(default_factory=tuple)


def parse_manifest_json(obj: Mapping[str, Any]) -> ManifestMetadata:
    """Parse a manifest object without assuming a rigid schema."""

    if not isinstance(obj, Mapping):
        raise TypeError("Manifest must be a mapping.")

    v2_partitions = _partitions_from_manifest_v2(obj)
    if v2_partitions is not None:
        return ManifestMetadata(raw=obj, partitions=v2_partitions)

    partitions: list[ManifestPartition] = []
    seen: set[tuple[str | None, str | None, str | None, str | None, str | None]] = set()

    def add_partition(partition: ManifestPartition) -> None:
        key = (
            partition.exchange,
            partition.stream,
            partition.symbol,
            partition.date,
            partition.path,
            partition.available,
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
    """Load a JSON manifest through Spark and parse it.

    This function is intended for Databricks execution. Local tests should pass a
    fake Spark object and must not read real S3.
    """

    payload = _read_text_file_with_spark(spark, manifest_path)
    if not payload.strip():
        raise ValueError(f"Manifest at {manifest_path} is empty.")

    try:
        loaded = json.loads(payload)
    except JSONDecodeError as exc:
        preview = payload[:500].replace("\n", "\\n")
        raise ValueError(
            f"Manifest at {manifest_path} is not valid JSON. "
            f"chars={len(payload)}, preview={preview!r}"
        ) from exc
    return parse_manifest_json(loaded)


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


def _partitions_from_manifest_v2(obj: Mapping[str, Any]) -> tuple[ManifestPartition, ...] | None:
    dates = obj.get("dates")
    if not isinstance(dates, Mapping):
        return None

    partitions: list[ManifestPartition] = []
    for date, date_entry in dates.items():
        if not isinstance(date, str) or not isinstance(date_entry, Mapping):
            continue
        exchanges = date_entry.get("exchanges")
        if not isinstance(exchanges, Mapping):
            continue

        for exchange, exchange_entry in exchanges.items():
            if not isinstance(exchange, str) or not isinstance(exchange_entry, Mapping):
                continue
            streams = exchange_entry.get("streams")
            if not isinstance(streams, Mapping):
                continue

            for stream, stream_entry in streams.items():
                if not isinstance(stream, str) or not isinstance(stream_entry, Mapping):
                    continue
                symbols = stream_entry.get("symbols")
                if not isinstance(symbols, Mapping):
                    continue

                for symbol, symbol_entry in symbols.items():
                    if not isinstance(symbol, str) or not isinstance(symbol_entry, Mapping):
                        continue
                    artifacts = _string_artifacts(symbol_entry.get("artifacts"))
                    data_key = artifacts.get("data_key")
                    available = symbol_entry.get("available")
                    partitions.append(
                        ManifestPartition(
                            exchange=exchange,
                            stream=stream,
                            symbol=symbol,
                            date=date,
                            path=data_key,
                            available=available if isinstance(available, bool) else None,
                            artifacts=artifacts,
                        )
                    )

    return tuple(partitions)


def _string_artifacts(value: Any) -> Mapping[str, str]:
    if not isinstance(value, Mapping):
        return {}
    return {key: item for key, item in value.items() if isinstance(key, str) and isinstance(item, str)}


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


def _read_text_file_with_spark(spark: Any, path: str) -> str:
    if hasattr(spark, "_jvm") and hasattr(spark, "_jsc"):
        return _read_text_file_with_hadoop(spark, path)

    rows = spark.read.text(path).collect()
    return "\n".join(_row_value(row) for row in rows)


def _read_text_file_with_hadoop(spark: Any, path: str) -> str:
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    filesystem = hadoop_path.getFileSystem(hadoop_conf)
    stream = filesystem.open(hadoop_path)

    try:
        scanner = jvm.java.util.Scanner(stream, "UTF-8").useDelimiter("\\A")
        try:
            if scanner.hasNext():
                return scanner.next()
            return ""
        finally:
            scanner.close()
    finally:
        stream.close()


def _row_value(row: Any) -> str:
    if isinstance(row, Mapping):
        value = row["value"]
    else:
        value = row.value

    if not isinstance(value, str):
        raise TypeError("Spark text row value must be a string.")
    return value
