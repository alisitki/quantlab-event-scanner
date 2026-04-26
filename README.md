# quantlab-event-scanner

Phase 0 repository bootstrap for the QuantLab event scanner.

This repository currently contains only the project skeleton: package code,
configuration, tests, a Databricks Asset Bundle placeholder, and packaging files.
It does not scan S3, execute Databricks jobs, run Spark event detection, train ML
models, perform trading or execution, create notebooks, or store data locally.

Input data is expected to live on S3 in future phases. Compacted metadata will be
read from `s3://quantlab-compact-stk-euc1/compacted/_manifest.json` in future
Databricks/Spark workflows. Output data must also be written to S3. The local PC
must not store data outputs.

The Databricks CLI will be required later for bundle validation, deployment, and
job execution:

```bash
databricks bundle validate
databricks bundle deploy
databricks bundle run
```

## Phase 0.5 - Databricks CLI authentication

The repository code can exist locally without Databricks CLI authentication.
Databricks CLI authentication is required before running bundle validation,
deployment, or jobs from this machine. Authentication must be performed manually
by the user. Do not commit credentials, tokens, workspace credentials, or
`.databrickscfg`; do not store tokens in this repository.

Install the Databricks CLI on macOS:

```bash
brew tap databricks/tap
brew install databricks
databricks -v
```

Authenticate with the development workspace:

```bash
databricks auth login --host <DATABRICKS_WORKSPACE_URL> --profile quantlab-dev
databricks auth profiles
databricks current-user me --profile quantlab-dev
```

Replace `<DATABRICKS_WORKSPACE_URL>` with the real workspace URL. The preferred
local profile name is `quantlab-dev`. A production profile can be added later as
`quantlab-prod`.

Validate the bundle after authentication:

```bash
databricks bundle validate -t dev --profile quantlab-dev
```

Bundle deploy and run commands are not part of Phase 0.5.

Future phases:

1. Phase 1: event map scan
2. Phase 2: pre-event window extraction
3. Phase 3: normal-time comparison

## Local Development

Install the package with development dependencies:

```bash
python -m pip install -e ".[dev]"
```

Run tests:

```bash
pytest
```
