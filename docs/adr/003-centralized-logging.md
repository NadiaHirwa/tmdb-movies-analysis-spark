# ADR 003: Centralized logging module over `print()` statements

## Status
Accepted

## Context
The original scripts used `print()` for all status output, with no distinction between informational messages, warnings, and errors, and no persistent record of what happened during a run. Separately, printing certain non-ASCII text (language names in scripts other than Latin) caused an unhandled `UnicodeEncodeError` under Windows' default console encoding.

## Decision
A single `get_logger(name)` function, defined once in `src/monitoring/logging_config.py`, is used by every module in the project. Each logger is named after its module (e.g. `ingestion.api`), writes to both the console and a shared `logs/pipeline.log` file, and the file handler uses explicit UTF-8 encoding.

## Consequences
- Every module's output is consistently formatted and timestamped.
- Log severity (`INFO`, `WARNING`, `ERROR`) is now meaningful and filterable, rather than every message looking identical.
- A permanent, reviewable record of each run exists in `logs/pipeline.log`, independent of what happened to be visible in the console at the time.
- The UTF-8 file handler resolves the encoding crash for the log file; console rendering of some Unicode characters may still vary by terminal, since that is a broader Windows console limitation outside the application's control.

## Alternatives Considered
A third-party structured-logging library (e.g. `structlog`) was considered for richer, machine-parsable log output. Python's built-in `logging` module was chosen instead, since it fully covers this project's needs (console + file output, severity levels, per-module naming) without adding an external dependency.