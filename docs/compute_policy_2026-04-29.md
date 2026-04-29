# Compute Policy - 2026-04-29

## Current Quota Boundary

The current known project maximum available classic cluster profile is:

- Driver: `r6i.4xlarge`
- Workers: `28 x r6i.2xlarge`
- Photon: enabled when available
- Availability: spot workers are acceptable for research runs when retry cost is
  acceptable; keep one on-demand driver

This is a quota boundary and an allowed upper profile, not a default for every
job.

## Runtime Preference

The project preference is to use enough compute to keep research runs short.
When the needed size is uncertain, choose a conservative fast profile inside
the current quota rather than a tiny cluster that risks multi-hour runtime.

Do not use very small clusters such as one `m5d.large` worker for heavy Spark
research jobs. Historical Phase 2J notes show that small compute contributed to
long runtimes.

## Recommended Profiles By Work Type

| Work type | Examples | Recommended starting profile |
|---|---|---|
| Metadata/probe/log-only | manifest probe, small coverage checks | serverless or small classic |
| Small persisted-artifact review | Phase 3B review over accepted Phase 3A outputs | `r6i.xlarge` or `r6i.2xlarge` driver, `2-4 x r6i.xlarge/r6i.2xlarge` workers |
| Single-event or small trial transforms | Phase 2A-2G style trial jobs | `r6i.2xlarge` driver, `4-8 x r6i.xlarge/r6i.2xlarge` workers |
| Multi-normal / multi-event generation | Phase 2J, Phase 3A Job A-style generation | `r6i.4xlarge` driver, `8-28 x r6i.2xlarge` workers depending on scope |
| Accepted-scale heavy run | Phase 3A accepted-scale rerun or similar | Up to current max: `r6i.4xlarge` driver, `28 x r6i.2xlarge` workers |

## Phase 3B Guidance

Phase 3B reads already-persisted Phase 3A comparison/top-diff/summary parquet
outputs and writes small review tables. It should not need the maximum profile.

Use a small-to-medium classic cluster if available:

- Driver: `r6i.xlarge` or `r6i.2xlarge`
- Workers: `2-4 x r6i.xlarge` or `2-4 x r6i.2xlarge`
- Photon: enabled
- Autoscale: not required

If queue time or runtime matters more than cost for the manual review run, a
larger cluster inside the quota is acceptable, but it should still be recorded
in the run result.

## Run Logging Rule

Every accepted runbook/result document should record:

- Databricks job/task run id
- Driver node type
- Worker node type
- Worker count
- Photon enabled/disabled
- Spot/on-demand mix
- Runtime duration
- Whether the run used the current max quota profile

This keeps future compute choices grounded in measured runtime rather than
guesswork.
