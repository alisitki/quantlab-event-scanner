#!/usr/bin/env python
"""Phase 1A manifest latest BTC trade probe.

This job intentionally stops after reading selected parquet files and logging
schema, row count, and a small sample. It does not run event detection or write
outputs.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path


def _script_path() -> Path | None:
    script_name = globals().get("__file__") or globals().get("filename")
    if not isinstance(script_name, str):
        return None
    return Path(script_name).resolve()


def _bootstrap_src_path() -> None:
    job_file = _script_path()
    candidates = [Path.cwd() / "src"]
    if job_file is not None:
        candidates.insert(0, job_file.parents[1] / "src")

    for candidate in candidates:
        if candidate.exists():
            sys.path.insert(0, str(candidate))
            return


_bootstrap_src_path()

from quantlab_event_scanner.config import load_config  # noqa: E402
from quantlab_event_scanner.manifest import load_manifest_from_s3_with_spark  # noqa: E402
from quantlab_event_scanner.probe import (  # noqa: E402
    latest_partition_date,
    partition_paths,
    select_btc_trade_partitions,
)


LOGGER = logging.getLogger("quantlab_event_scanner.phase1_probe")


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    config_path = _resolve_path(args.config)
    config = load_config(config_path)

    spark = _get_spark_session()
    manifest = load_manifest_from_s3_with_spark(spark, config.manifest_path)
    latest_date = latest_partition_date(manifest)
    selected = select_btc_trade_partitions(manifest, date=latest_date)
    paths = partition_paths(selected, input_root=config.input_root)

    LOGGER.info("Loaded manifest: %s", config.manifest_path)
    LOGGER.info("Manifest partition count: %s", len(manifest.partitions))
    LOGGER.info("Latest manifest date: %s", latest_date)
    LOGGER.info("Selected BTC trade partition count: %s", len(selected))
    LOGGER.info("Selected parquet paths from manifest artifacts.data_key:")
    for path in paths:
        LOGGER.info("  %s", path)

    frame = spark.read.parquet(*paths)

    LOGGER.info("Parquet schema:")
    frame.printSchema()

    row_count = frame.count()
    LOGGER.info("Parquet row count: %s", row_count)

    LOGGER.info("Parquet sample, limit=%s:", args.sample_size)
    frame.limit(args.sample_size).show(truncate=False)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe latest BTC trade parquet data.")
    parser.add_argument("--config", default="configs/dev.yaml")
    parser.add_argument("--sample-size", type=int, default=20)
    return parser.parse_args()


def _resolve_path(path: str) -> Path:
    candidate = Path(path)
    if candidate.exists():
        return candidate

    bases = [Path.cwd()]
    job_file = _script_path()
    if job_file is not None:
        bases.insert(0, job_file.parents[1])

    for base in bases:
        resolved = base / path
        if resolved.exists():
            return resolved

    raise FileNotFoundError(f"Config file not found: {path}")


def _get_spark_session():
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


if __name__ == "__main__":
    main()
