# Local End-to-End Execution

This document outlines how to run a full end-to-end (E2E) workflow locally using the file-based (`file://`) adapters and the CLI orchestration tools included in this repository.

## Overview

Local E2E runs simulate production execution while keeping dependencies minimal. The setup uses:

- File-backed storage adapters (`file://data/...`) for inputs and outputs.
- The local orchestrator adapter that schedules pipelines via the CLI.
- In-memory or `.env`-based secret adapters for credentials.

## Prerequisites

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Ensure the sample data set is available:

```bash
make seed-sample-data
```

## Configure the Bundle

1. Copy the sample bundle:
   ```bash
   cp cfg/bundles/dev.yaml cfg/bundles/local-e2e.yaml
   ```
2. Update `cfg/bundles/local-e2e.yaml` to use file-based adapters:
   ```yaml
   adapters:
     orchestrator:
       type: local-cli
     storage:
       type: filesystem
       base_path: file://data/local
     secrets:
       type: dotenv
       path: .env.local
   ```
3. Add any pipeline-specific overrides in `cfg/overrides/local-e2e.yaml` if required.

## Launch the Orchestrator

The local orchestrator translates bundle definitions into CLI invocations.

```bash
scripts/orchestrator.py \
  --bundle cfg/bundles/local-e2e.yaml \
  --watch
```

- `--watch` monitors the filesystem for new inputs under `data/local/inbox/` and triggers the corresponding pipelines.
- Logs are written to `run/logs/local/` for inspection.

## Execute a Pipeline Manually

You can also drive a pipeline directly:

```bash
scripts/run_pipeline.py \
  --bundle cfg/bundles/local-e2e.yaml \
  --pipeline ingest-customers \
  --input file://data/local/inbox/customers.csv \
  --output file://data/local/outbox/customers.parquet
```

The CLI resolves configuration in the following order:

1. Command-line flags passed to `run_pipeline.py`.
2. Environment variables prefixed with `MVP_CFG__`.
3. Overrides in `cfg/overrides/local-e2e.yaml`.
4. Base bundle at `cfg/bundles/local-e2e.yaml`.

## Validate Results

1. Inspect the output files:
   ```bash
   ls data/local/outbox
   ```
2. Tail the orchestrator logs:
   ```bash
   tail -f run/logs/local/orchestrator.log
   ```
3. Run automated assertions:
   ```bash
   pytest tests/e2e/test_local_pipeline.py -k ingest_customers
   ```

## Cleanup

After testing, reset the workspace:

```bash
rm -rf data/local/outbox/*
rm -rf run/logs/local/*
```

This restores the environment for the next E2E iteration.
