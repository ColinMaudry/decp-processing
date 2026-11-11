# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`decp-processing` consolidates French public procurement data ("DECP" — Données Essentielles de la Commande Publique) from many heterogeneous sources on data.gouv.fr (and direct scrapes of platforms like AWS, DUME, marches-securises.fr, Klekoon), normalizes the two DECP formats (2019 and 2022), enriches with SIRENE company data, and publishes the result as parquet/CSV/SQLite back to data.gouv.fr. Output powers the [decp.info](https://decp.info) site.

## Common commands

Install dev environment:

```bash
pip install -e .[dev]      # use .'[dev]' under zsh
pre-commit install
```

Run the main pipeline locally (uses an ephemeral Prefect server when `PREFECT_API_URL` is empty):

```bash
python run_flow.py sirene_preprocess   # REQUIRED once before anything else — produces SIRENE_DATA_DIR
python run_flow.py decp_processing     # main pipeline
python run_flow.py scrap               # uses SCRAPING_TARGET / SCRAPING_MODE / SCRAPING_YEAR / SCRAPING_MONTH env vars
python run_flow.py get_cog
```

Tests (also depend on `sirene_preprocess` having been run at least once):

```bash
pytest                          # all tests
pytest tests/test_main.py       # full pipeline integration test (slowest)
pytest tests/test_clean.py::test_function_name   # single test
```

`pyproject.toml` injects test-only env vars under `[tool.pytest.ini_options]` (notably `DATASETS_REFERENCE_FILEPATH=tests/data/source_datasets_test.json`, `DECP_PROCESSING_PUBLISH=`, `DECP_USE_CACHE=false`) — production `.env` values are bypassed during tests.

Deploy Prefect flows (requires `PREFECT_API_URL` set in `.env`):

```bash
python src/deployments.py
prefect deployment run decp-processing      # trigger immediately
```

Lint / format runs through pre-commit (ruff + ruff-format + prettier + custom import check). Run manually before commits:

```bash
pre-commit run --all-files
```

## Architecture

### Entry points and orchestration

- `run_flow.py` — thin CLI dispatching to one of the flows in `src/flows/`. Use this for local runs.
- `src/flows/` — Prefect `@flow` definitions. Each is a top-level pipeline:
  - `decp_processing.py` — main daily pipeline (list resources → batch download/clean → concat → enrich with SIRENE → write parquet/CSV → optionally publish).
  - `sirene_preprocess.py` — monthly job; produces parquet of SIRENE unités légales + établissements under `SIRENE_DATA_DIR` (one folder per month). The main flow auto-triggers this if the directory for the current month is missing.
  - `scrap.py` — dispatches to one of the per-platform scrapers in `src/tasks/scrap/` based on `SCRAPING_TARGET`. Modes: `month` (default), `year`, `all`.
  - `get_cog.py` — fetches the Code Officiel Géographique (commune codes).
- `src/deployments.py` — declares Prefect deployments with cron schedules (prod runs `cron="0 5 * * *"` on `main` branch from GitHub). Edits to schedules/env vars happen here, not in flows.

### Tasks layer

`src/tasks/` holds the reusable Prefect `@task` functions that the flows compose:

- `get.py` — streaming downloads (`stream_get`) and parsers for DECP JSON (`ijson`-based) and XML (`lxml`), plus SIRENE downloads. The 2019 vs 2022 DECP formats are routed via `src.config.DecpFormat` and the schemas in `src/schemas.py`.
- `clean.py` — record-level cleaning, character normalization, struct flattening.
- `transform.py` — polars LazyFrame transforms (sorting modifications, concatenating partial parquets, NAF/CPV matching, etc.). The `apply_modifications` logic produces one row per state-of-the-marché after each modification — this is core to how the output models contract amendments.
- `enrich.py` — joins the consolidated frame against SIRENE and adds derived columns (`add_duree_restante`, `add_type_marche`).
- `output.py` — `sink_to_files` writes parquet (and CSV) via `pl.LazyFrame.sink_*` for streaming output; `generate_final_schema` produces the frictionless table schema.
- `publish.py` — pushes resources to data.gouv.fr via its API (gated on `DECP_PROCESSING_PUBLISH` env + a >5000-resources sanity check + empty `SOLO_DATASETS` — see `decp_processing.py`; this triple gate exists specifically to prevent test runs from publishing).
- `dataset_utils.py` — list resources of a tracked dataset.
- `utils.py` — logger, cache management (`remove_unused_cache`), artifact helpers, stats generation.
- `scrap/{aws,dume,klekoon,marches_securises}.py` — one file per scraped platform.

### Configuration

`src/config.py` is the single source of truth for runtime config:

- Loads `.env` automatically; if missing, copies `template.env` → `.env` so first-time runs don't break.
- `TRACKED_DATASETS` is parsed from `reference/source_datasets.json` (or override via `DATASETS_REFERENCE_FILEPATH`); entries with `"skip": true` are excluded unless explicitly listed in `SOLO_DATASETS`.
- `EXCLUDED_RESOURCES` always includes a hard-coded list of known-broken/non-conformant resources from data.gouv.fr — extend this list rather than working around bad data downstream.
- Cache: parquet files keyed by checksum in `RESOURCE_CACHE_DIR`; `DECP_USE_CACHE` and `CACHE_EXPIRATION_TIME_HOURS` control reuse.
- `BASE_DF_COLUMNS` comes from `reference/schema_base.json` and is the canonical output column set; `COLUMNS_TO_DROP` lists fields intentionally not propagated (concessions, sub-treatance, format-specific noise).

### Reference data

`reference/`:

- `source_datasets.json` — list of data.gouv.fr dataset IDs and metadata that the pipeline pulls from.
- `schema_base.json` — frictionless table schema describing the canonical output columns.

### Tests

- Integration test `tests/test_main.py` runs the entire `decp_processing` flow inside `prefect_test_harness` against test data referenced by `tests/data/source_datasets_test.json`. It is slow and depends on a prior `sirene_preprocess` run.
- Unit-ish tests in `test_clean.py`, `test_enrich.py`, `test_stats.py`, `test_transform.py` exercise polars transforms directly.

## Project conventions

- **Imports inside `src/` must be absolute from the package root**: `from src.tasks.X import Y`, not `from tasks.X import Y`. Enforced by `script/check_imports.py` in pre-commit. The `pythonpath = ["src"]` setting in `pyproject.toml` is for pytest only — production code goes through the `src.` prefix.
- DECP comes in **two distinct formats** (2019 and 2022). Adding a field usually means updating both `SCHEMA_MARCHE_2019` and `SCHEMA_MARCHE_2022` in `src/schemas.py`, plus the cleaning/transform paths.
- Polars is used in **lazy/streaming mode** (`scan_parquet`, `sink_parquet`) to handle the full corpus without loading it all in memory; prefer `LazyFrame` over `DataFrame` when extending the pipeline.
- Adding a new data source = a new entry in `reference/source_datasets.json` (and possibly a new scraper under `src/tasks/scrap/` if not on data.gouv.fr).
- Prefect cache and result storage live in `prefect_storage/` and `prefect.db` (gitignored, large).
