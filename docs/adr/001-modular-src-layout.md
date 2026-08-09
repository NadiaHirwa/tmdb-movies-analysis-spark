# ADR 001: Modular `src/` layout over monolithic scripts

## Status
Accepted

## Context
The initial version of this project had two flat scripts, `fetch_data.py` and `clean_data.py`, with all logic — including KPI ranking and chart generation — written directly inline in the analysis notebook. This made the code difficult to test, and meant fetch/clean logic ran automatically as a side effect of importing the file, rather than only when explicitly invoked.

## Decision
Restructure the project into `src/ingestion`, `src/transformations`, `src/analysis`, `src/visualization`, and `src/monitoring`, each owning exactly one concern. Every module exposes its logic through functions with docstrings, guarded by `if __name__ == "__main__":`, so importing a module never triggers its logic automatically.

## Consequences
- Each module can be unit tested independently (see `tests/test_cleaning.py`).
- `main.py` can orchestrate the full pipeline by importing and calling functions from each module, without duplicating any logic.
- The notebook now imports from `src/` rather than redefining logic inline, so there is exactly one source of truth for any given function.
- Slightly more boilerplate (explicit `sys.path` setup per module) than a flat script layout would require.

## Alternatives Considered
A larger production-scale layout (Docker, multiple environment configs, database/S3 storage backends) was considered, based on a template shared by the module lead. It was not adopted, since this project has no deployment target, no database, and a small, fixed input size — that scaffolding would add complexity without adding value here. The current structure keeps the separation-of-concerns principle from that template while staying proportional to the project's actual scope.