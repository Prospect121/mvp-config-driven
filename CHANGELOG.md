# Changelog

## [0.2.1] - 2025-10-25

### Highlights
- Finance pipelines now auto-discover the JDBC raw source when `RAW_SOURCE=jdbc`, simplifying multi-channel ingestion toggles.
- Multi-cloud production runbooks reference the new `cfg/finance/**/*.prod.yml` configurations for Databricks, AWS, GCP, and Azure.

### Tooling & CI
- Hardened `auth-lint` by enforcing incremental `state_id` requirements and blocking embedded `Authorization` headers or hardcoded credentials in production YAMLs.
- Added a `smoke-finance` CI job that dry-runs the finance pipeline for both HTTP and JDBC sources and uploads `smoke-finance.log` as an artifact.

### Configuration
- Published finance production configurations per cloud provider, leveraging managed identities and environment-scoped parameters.

### Documentation
- Documented the finance production rollout process per cloud, including Databricks job JSON definitions and Glue/Dataproc/Synapse commands.

## [0.2.0] - 2025-02-18

### Highlights
- Raw layer runner enabled by default with production-ready storage fallbacks across AWS, Azure, and GCP.
- End-to-end smoke workflows now cover multi-cloud production configurations with forced dry-run validation in CI.
- Enforced no-Docker policy baked into CI alongside cross-layer, TLS, and legacy cleanup guards.

### Configuration
- Added identity-based production YAMLs for raw, bronze, silver, and gold layers per cloud provider.
- Introduced managed identity environment manifests to avoid embedding static keys.
- Documented orchestration templates for Databricks, AWS Glue/EMR, Azure Synapse/ADF, and GCP Dataproc using v0.2.0 wheel artifacts.

### Tooling & CI
- Added `smoke-prod` GitHub Actions job that forces dry-run execution of every `*.prod.yml` configuration.
- Published reusable submission script and workflow templates for cloud orchestrators.
- Declared required branch protection checks and CODEOWNERS for production configs.

### Documentation
- Added identity credential guidance to the README.
- Refreshed runbooks to reference v0.2.0 artifacts and prod YAML locations.

[0.2.0]: https://example.com/releases/v0.2.0
