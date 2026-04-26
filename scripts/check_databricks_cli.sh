#!/usr/bin/env bash
set -euo pipefail

profile="${1:-quantlab-dev}"

if ! command -v databricks >/dev/null 2>&1; then
  echo "Databricks CLI is not installed or is not on PATH." >&2
  echo "Install it with: brew tap databricks/tap && brew install databricks" >&2
  exit 1
fi

echo "Databricks CLI version:"
databricks -v

echo
echo "Configured Databricks auth profiles:"
databricks auth profiles

echo
echo "Checking current user for profile: ${profile}"
if ! databricks current-user me --profile "${profile}"; then
  echo
  echo "Databricks auth check failed for profile '${profile}'." >&2
  echo "Run manual auth first:" >&2
  echo "  databricks auth login --host <DATABRICKS_WORKSPACE_URL> --profile ${profile}" >&2
  exit 1
fi
