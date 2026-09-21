# AGENTS.md — api-platform

## Working agreement (learning-first)

This is a learning project. Optimize for my understanding, not for speed.

- **Teach alongside every change.** For each major step explain the concept, the
  architecture, and *why* a decision was made before/while showing code.
- **Stage by stage.** Do not jump ahead or implement future stages. Work the
  phases in order (see Phase 2 plan) and stop for confirmation between stages.
- **I want to contribute.** Handle repetitive/boilerplate work, but leave the
  meaningful decisions, small edits, and reasoning to me. Ask me to make the
  interesting calls rather than doing everything.
- **No copy-paste black boxes.** After each stage I should be able to explain
  the result to someone else, modify it, and extend it.
- **Flag trade-offs and gotchas** explicitly (idempotency, cost, least
  privilege, failure modes).

## Project

Dagster ELT platform. Phase 1 = GitHub API → local JSON.
Phase 2 = GitHub → Dagster → GCS → BigQuery (asset chain: raw → parquet → bq).

## Environment facts

- Code lives in WSL (`Ubuntu`) at `/home/david/api-platform`; Docker daemon is in WSL.
- `gcloud` runs from **Windows** (account `davidmudiwa13@gmail.com`). Running it
  from WSL sees a different config, so do all provisioning from Windows.
- GCP project: `kestra-sandbox-492709`, region `us-central1`.
- Org policy `iam.disableServiceAccountKeyCreation` blocks SA JSON keys.
  Container authenticates with **user ADC** copied to `secrets/adc.json`
  (gitignored + dockerignored), via `GOOGLE_APPLICATION_CREDENTIALS` in `.env`.
- Service account `api-platform-dev@kestra-sandbox-492709.iam.gserviceaccount.com`
  reserved for a future keyless production path; currently has BigQuery
  dataEditor + jobUser (GCS role added bucket-scoped in Step 3).
- Phase 2 GCS bucket: `gs://github-data-492709` (region us-central1, uniform
  bucket-level access, public access prevention, lifecycle
  raw/ only: STANDARD→NEARLINE@30d→COLDLINE@90d→ARCHIVE@365d; processed/ stays
  STANDARD, versioning off).
  Config kept in `infra/gcs-lifecycle.json`.
- SA is granted `roles/storage.objectAdmin` **bucket-scoped**; user
  `davidmudiwa13@gmail.com` has `roles/iam.serviceAccountTokenCreator` on the SA
  (enables keyless impersonation).
- Pre-existing resources (unrelated): bucket `ecommerce_product_files`,
  BQ datasets `products_raw`, `takealot_raw`.
- Named volume `elt_platform_venv` mounts at `/app/.venv` and shadows image
  rebuilds — recreate with `docker compose down -v` or `uv sync` in-container.
- Dagster auto-loads `.env` and its values **override** the process env, so the
  container `DAGSTER_HOME=/app/.dagster` breaks local CLI runs (`dagster
  definitions validate`, `dagster dev`). Use the Python import checks below, or
  run inside Docker.
- `EnvVar` defaults are resolved by Dagster's config system, not by pydantic:
  instantiating e.g. `GCSResource()` directly in plain Python leaves the field
  as the literal env-var name. Pass explicit values in standalone scripts.

## Commands

- Smoke-test cloud auth: `uv run python scripts/verify_gcp.py`
- Smoke-test the GCS resource round-trip: `uv run python scripts/check_gcs_resource.py`
- Load/validate Definitions without the Dagster CLI: `uv run python scripts/validate_defs.py`
- Materialize the repositories asset locally (hits GitHub + GCS): `uv run python scripts/run_repositories_asset.py`
- Add deps: `uv add <pkg>` (update `uv.lock`; Docker build uses `uv sync --frozen`)

## Layout

- `assets/` — Dagster assets (Phase 1 extraction lives in `assets/github.py`)
- `resources/` — injectable external-system handles (e.g. `GCSResource`)
- `infra/` — cloud config as code (e.g. `gcs-lifecycle.json`)
- `scripts/` — standalone verification/smoke-test helpers
- `secrets/` — gitignored + dockerignored credentials (ADC)
