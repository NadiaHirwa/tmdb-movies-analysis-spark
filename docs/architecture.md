# Architecture & Design Decisions

## Why this structure

The project is split into `ingestion`, `transformations`, `analysis`, `visualization`, `pipelines`, `monitoring`, and `utils` — each folder owns exactly one concern. This means:
- Each piece can be tested and modified independently.
- `main.py` is a thin entry point: it creates the `SparkSession`, parses CLI flags, and delegates the actual orchestration to `src/pipelines/movie_pipeline.py` - kept separate so the same pipeline logic could, in principle, be triggered from a notebook or an orchestration tool (e.g. Airflow) without duplicating this sequence.
- The notebook (`notebooks/analysis.ipynb`) imports from these modules rather than redefining logic inline, so there is exactly one source of truth for any given function.
- `src/utils/constants.py` holds shared values (e.g. `MOVIE_IDS`) referenced by multiple modules, so there is exactly one place to update them.

This structure follows the same modular philosophy as the [pandas version of this project](../../tmdb-movies-analysis) (see its ADR-001), extended with a `pipelines/` module now that orchestration is non-trivial enough to warrant its own file.

## Pipeline flow

```
                    ┌─────────────────┐
                    │   TMDb API      │
                    └────────┬────────┘
                             │  requests + retry logic
                             ▼
                 ┌───────────────────────┐
                 │  src/ingestion/api.py │
                 │  -> data/raw/*.json   │
                 └───────────┬───────────┘
                             │
                             ▼
          ┌──────────────────────────────────────┐
          │  src/transformations/cleaning.py      │
          │  Spark reads JSON -> extracts nested   │
          │  structs -> casts types -> filters      │
          │  -> data/processed/*.parquet            │
          └───────────────────┬──────────────────┘
                              │
                              ▼
            ┌──────────────────────────────────┐
            │   src/analysis/kpis.py            │
            │   rankings, search, groupBy       │
            │   aggregations (all lazy Spark    │
            │   transformations)                │
            └────────────────┬──────────────────┘
                              │  .toPandas()  <- the one deliberate
                              │                  Spark -> pandas boundary
                              ▼
           ┌───────────────────────────────────┐
           │  src/visualization/visualize.py    │
           │  Matplotlib charts -> outputs/*.png │
           └────────────────────────────────────┘

  Orchestrated end-to-end by src/pipelines/movie_pipeline.py,
  triggered by main.py or notebooks/analysis.ipynb
```

## Why Spark instead of pandas

This project is a PySpark rewrite of an [earlier pandas-based version](../../tmdb-movies-analysis), built to practice distributed DataFrame processing. On this project's actual 18-movie dataset, Spark provides no real performance benefit - pandas would be faster for data this small, since Spark's parallelization overhead outweighs its benefits below a certain scale. The value here is in the exercise itself: the cleaning and analysis logic is written using Spark's DataFrame API, lazy evaluation model, and immutable-DataFrame semantics, exactly as it would be for a genuinely large dataset.

Notable differences from the pandas version, driven by Spark's design (see inline comments in `cleaning.py` and `kpis.py` for the specific translations):
- No index: Spark DataFrames have no row index, so there is no `.reset_index()` equivalent.
- No row-wise operations: pandas' `df.notnull().sum(axis=1)` has no direct Spark equivalent, since Spark operates column-by-column across all rows, never "across" a single row (see `filter_sparse_rows()`).
- `F.when(...).otherwise(...)` replaces pandas' `.replace()` and `.loc[condition, col] = value`, since Spark DataFrames are immutable and cannot be updated in place.
- Median has no exact, cheap equivalent (`percentile_approx` is used instead), since an exact median requires a full sort across however many machines the data might be distributed over.

## Error handling strategy

`src/ingestion/api.py` distinguishes between three failure categories, since they require different responses:

1. **Invalid resource (HTTP 404)** - not retried. No amount of retrying makes an invalid movie ID valid. Detected via the actual HTTP status code returned by the API, not inferred from the shape of the response body.
2. **Transient failures (timeouts, 5xx server errors, network errors)** - retried up to 3 times with linear backoff (1s, 2s, 3s), since these may resolve on their own.
3. **Unrecoverable failures after all retries** - logged as an error and the individual movie is skipped, rather than crashing the entire ingestion run.

`main.py` adds one further layer: a top-level `try/except` around the entire pipeline run, which logs any completely unexpected failure with its full traceback and exits with a non-zero status code. A `finally` block ensures `spark.stop()` always runs, releasing Spark's resources whether the pipeline succeeds or fails.

## Logging strategy

A single `get_logger()` function (`src/monitoring/logging_config.py`) is used by every module, guaranteeing consistent log formatting and behavior throughout the project. Logs are written to both the console and `logs/pipeline.log`, with explicit UTF-8 encoding for the same reason as the pandas version (non-ASCII text in the dataset).

## Data quality decisions

These decisions carry over unchanged from the pandas version's reasoning, now implemented with Spark's DataFrame API:

- **Alphabetical sorting of extracted multi-value fields** (genres, countries, companies, languages, directors): prevents the same real-world combination from being represented as different strings depending on API response order.
- **Cast order is preserved, not sorted**: TMDb's `order` field carries real meaning (billing prominence).
- **Zero values treated as missing**: budget, revenue, and runtime of exactly 0 reflect missing data, not real values.
- **`vote_average` is set to null wherever `vote_count` is 0**: a rating computed from zero votes carries no real signal.

## Output format: Parquet instead of CSV

See [ADR-007](adr/007-parquet-over-csv-for-spark-output.md) for the full reasoning. In short: Parquet preserves the schema (dates and numeric types stay typed, rather than being flattened to text), is Spark's native optimized format, and is consistent with the file format underlying Iceberg/Delta/Hudi covered elsewhere in this course module.

## Reproducibility

- All dependencies are pinned to exact versions in `requirements.txt`, including the specific PySpark version.
- `data/raw/`, `data/processed/`, `outputs/`, and `logs/` directories are created automatically at runtime if they don't already exist.
- `main.py --skip-fetch` allows the pipeline to be re-run against the last successfully fetched data, without depending on the TMDb API being available.
- `main.py --show-charts` displays charts interactively in addition to saving them; left off by default so the pipeline runs correctly in a non-interactive terminal.