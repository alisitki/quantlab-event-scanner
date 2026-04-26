#!/usr/bin/env bash
set -euo pipefail

target="${1:-dev}"
profile="${2:-quantlab-dev}"

if ! command -v databricks >/dev/null 2>&1; then
  echo "Databricks CLI is not installed or is not on PATH." >&2
  echo "Install it with: brew tap databricks/tap && brew install databricks" >&2
  exit 1
fi

echo "Validating Databricks bundle target '${target}' with profile '${profile}'."
databricks bundle validate -t "${target}" --profile "${profile}"
