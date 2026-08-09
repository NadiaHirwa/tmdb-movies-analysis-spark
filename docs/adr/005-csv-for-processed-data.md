# ADR 005: CSV for processed output over Parquet or a database

## Status
Accepted

## Context
Once the cleaned dataset is fully flattened (no remaining nested lists or dictionaries), a storage format needs to be chosen for `data/processed/movies_clean.csv`, which is read back by both `main.py`'s KPI stage and the analysis notebook.

## Decision
Store the cleaned dataset as CSV.

## Consequences
- CSV is human-readable and can be opened directly in any spreadsheet tool for a manual sanity check, which is valuable at this project's scale (18 rows).
- CSV does not preserve dtypes: `release_date` reverts to a plain string on save and must be explicitly re-parsed with `parse_dates=[...]` on load. This is a known, documented trade-off, not an oversight — it was discovered directly during development when a `.dt.year` call failed after a CSV round-trip, and the fix (`parse_dates`) is applied consistently everywhere the file is read.
- At 18 rows, file size and read/write performance are a non-issue; this decision would need revisiting at a much larger scale.

## Alternatives Considered
**Parquet** would preserve dtypes (including datetime) automatically and is the standard choice for larger, production-scale datasets, but is not human-readable and adds a dependency (`pyarrow` or `fastparquet`) that is not otherwise needed at this project's scale.
**A database (e.g. SQLite)** was also considered, but would add setup and query-interface overhead disproportionate to a fixed, 18-row dataset with no concurrent access or update requirements.