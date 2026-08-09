# ADR 006: No Docker or multi-environment config for this project

## Status
Accepted

## Context
A shared reference template for structuring data engineering projects included `Dockerfile`, `docker-compose.yml`, and per-environment config files (`config/dev.yaml`, `config/test.yaml`, `config/prod.yaml`).

## Decision
Do not adopt containerization or multi-environment configuration for this project. Dependencies are pinned in `requirements.txt`, and the only environment-specific value (the TMDb API key) is handled via a single `.env` file, documented with `.env.example`.

## Consequences
- Lower setup overhead: `pip install -r requirements.txt` plus a `.env` file is sufficient to run the full pipeline.
- The project cannot currently be deployed as a container to a scheduling system (e.g. Airflow, a cron-based server) without additional work.
- This decision is scoped to this project's current requirements (a one-off, locally-run analysis pipeline with a single, fixed dataset) and should be revisited if the project ever needs scheduled/automated execution, multiple environments (dev/staging/prod), or deployment outside a single developer's machine.

## Alternatives Considered
Full adoption of the shared template (Docker, docker-compose, per-environment YAML configs) was considered for consistency with team conventions, but was judged disproportionate to this project's actual scope: there is no database, no deployment target, and no need to run in more than one environment. The modular `src/` separation from ADR 001 was adopted from the same template without the infrastructure tooling that isn't yet needed.