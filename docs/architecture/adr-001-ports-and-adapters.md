# ADR-001: Ports and Adapters Architecture

- **Status:** Accepted
- **Date:** 2025-10-29

## Context

The MVP configuration-driven platform needs to ingest configuration bundles, orchestrate processing pipelines, and expose integrations with third-party services. Previous prototypes mixed domain logic with infrastructure concerns, making the system difficult to test, extend, and deploy across heterogeneous environments.

## Decision

Adopt a Ports and Adapters (Hexagonal) architecture with three primary layers:

1. **Domain Core** – Pure business rules implemented in `src/mvp_core` (entities, value objects, domain services). These components are infrastructure-agnostic and operate only on domain abstractions.
2. **Application Services** – Use cases living under `src/mvp_app` that orchestrate domain operations. They depend on ports that describe required infrastructure capabilities (e.g., configuration repositories, orchestration runners, secret stores).
3. **Adapters** – Infrastructure implementations located in `src/mvp_infra` and `tools/`. Adapters implement the ports for persistence, messaging, CLI orchestration, and provider-specific deployment tasks. They are composed via dependency injection using configuration defined in `cfg/` and `config/`.

Data flows from adapters into the domain through input ports, while application services invoke output ports to interact with external systems. This structure allows the CLI and automation scripts in `scripts/` to reuse domain and application services without importing infrastructure details directly.

## Rationale

- **Testability:** Domain and application layers can be tested with in-memory doubles by mocking the ports.
- **Replaceability:** Provider-specific adapters (AWS, Azure, GCP) can be swapped without changing domain logic.
- **Configurability:** Configuration-driven wiring lets deployments select adapters via YAML manifests, aligning with the project's goal of configurable behavior.
- **Parallel Development:** Teams can work on domain logic and adapters independently, converging through well-defined ports.

## Consequences

- Requires disciplined boundary management to prevent domain leakage of infrastructure concerns.
- Onboarding must include education about port interfaces and adapter registration patterns.
- Additional scaffolding is necessary for dependency injection and configuration resolution, but provides long-term maintainability benefits.

## Related Documents

- [`docs/config/config-spec.md`](../config/config-spec.md) — Defines configuration manifests that bind adapters to ports.
- [`docs/deploy`](../deploy) — Provider-specific deployment guides that rely on adapters implementing deployment ports.
