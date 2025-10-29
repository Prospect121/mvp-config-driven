# Configuration Specification

This document describes the configuration model used by the MVP configuration-driven platform. It covers the YAML manifest shape, the canonical JSON Schema, and the override resolution order that binds adapters to ports.

## YAML Manifest Structure

Configuration bundles live under `cfg/` and are typically loaded via the CLI (`scripts/configure.py`). Each bundle contains:

```yaml
version: 1
metadata:
  name: sample-pipeline
  environment: dev
  labels:
    owner: data-platform
adapters:
  orchestrator:
    type: airflow
    connection: file://orchestration/airflow.cfg
  storage:
    type: s3
    bucket: my-config-bucket
  secrets:
    type: aws-secrets-manager
    region: us-east-1
pipelines:
  - id: ingest-customers
    schedule: "0 * * * *"
    entrypoint: src/mvp_app/pipelines/customers:run
    inputs:
      - port: storage
        path: customers/raw/
    outputs:
      - port: storage
        path: customers/processed/
    hooks:
      on_success:
        - type: notifier
          port: orchestrator
          template: templates/success-email.j2
```

### Field Summary

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `version` | integer | yes | Configuration schema version. Used for compatibility checks. |
| `metadata` | object | yes | Identifiers and labels applied to deployments. |
| `adapters` | object | yes | Maps port names to adapter definitions. Each adapter declares a `type` and arbitrary settings. |
| `pipelines` | array | yes | List of pipeline definitions orchestrated by the application services. |
| `pipelines[].inputs` / `outputs` | array | no | Declares how pipelines consume and emit data through configured ports. |
| `pipelines[].hooks` | object | no | Optional lifecycle hooks bound to adapters. |

## JSON Schema

The following JSON Schema governs validation. It is shipped with the codebase at `config/schema/config.v1.json` and mirrored here for reference (abridged for readability).

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "MVP Configuration Bundle",
  "type": "object",
  "required": ["version", "metadata", "adapters", "pipelines"],
  "properties": {
    "version": {"type": "integer", "enum": [1]},
    "metadata": {
      "type": "object",
      "required": ["name", "environment"],
      "properties": {
        "name": {"type": "string"},
        "environment": {"type": "string"},
        "labels": {
          "type": "object",
          "additionalProperties": {"type": "string"}
        }
      },
      "additionalProperties": false
    },
    "adapters": {
      "type": "object",
      "minProperties": 1,
      "additionalProperties": {
        "type": "object",
        "required": ["type"],
        "properties": {
          "type": {"type": "string"}
        },
        "additionalProperties": true
      }
    },
    "pipelines": {
      "type": "array",
      "minItems": 1,
      "items": {
        "type": "object",
        "required": ["id", "entrypoint"],
        "properties": {
          "id": {"type": "string"},
          "schedule": {"type": "string"},
          "entrypoint": {"type": "string"},
          "inputs": {
            "type": "array",
            "items": {
              "type": "object",
              "required": ["port"],
              "properties": {
                "port": {"type": "string"},
                "path": {"type": "string"}
              },
              "additionalProperties": true
            }
          },
          "outputs": {"$ref": "#/properties/pipelines/items/properties/inputs"},
          "hooks": {
            "type": "object",
            "additionalProperties": true
          }
        },
        "additionalProperties": false
      }
    }
  },
  "additionalProperties": false
}
```

## Override Resolution

Configuration values can be overridden at runtime by combining multiple sources. The resolver applies the following precedence (highest priority first):

1. **Command-line flags** – `scripts/configure.py --set key=value` or `scripts/run_pipeline.py --override` inject values into the effective configuration.
2. **Environment variables** – Prefixed with `MVP_CFG__` and parsed into nested keys (double underscores become path separators).
3. **Profile-specific YAML** – Files named `<bundle>.<profile>.yaml` located in `cfg/overrides/` merge onto the base bundle when `--profile` is provided.
4. **Base bundle YAML** – The canonical configuration checked into version control.

Overrides are merged using deep dictionary semantics: scalars replace existing values, lists are replaced in full unless the CLI flag `--merge-lists` is passed (which concatenates unique entries), and objects are merged recursively. All overrides are validated against the JSON Schema before the configuration is instantiated into port adapters.
