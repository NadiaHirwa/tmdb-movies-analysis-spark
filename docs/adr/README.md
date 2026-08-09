# Architecture Decision Records

This folder records the significant technical decisions made while building this pipeline: the context that prompted each decision, what was decided, and what trade-offs were accepted.

| ADR | Title |
|---|---|
| [001](001-modular-src-layout.md) | Modular `src/` layout over monolithic scripts |
| [002](002-api-error-handling-strategy.md) | Explicit timeout, retry, and status-code checking for API calls |
| [003](003-centralized-logging.md) | Centralized logging module over `print()` statements |
| [004](004-sort-multivalue-fields.md) | Alphabetically sort extracted multi-value fields |
| [005](005-csv-for-processed-data.md) | CSV for processed output over Parquet or a database *(superseded by 007 for this Spark version)* |
| [006](006-no-containerization.md) | No Docker or multi-environment config for this project |
| [007](007-parquet-over-csv-for-spark-output.md) | Parquet over CSV for processed output (Spark version) |