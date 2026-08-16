# TMDB Movie Data Analysis (Spark)

![Tests](https://github.com/NadiaHirwa/tmdb-movies-analysis-spark/actions/workflows/tests.yml/badge.svg)

A data engineering pipeline that fetches movie data from the TMDb API, cleans and transforms it with PySpark, calculates business KPIs, and visualizes key trends. This is a PySpark rewrite of an [earlier pandas-based version](https://github.com/NadiaHirwa/tmdb-movies-analysis) of the same project, built as part of a Data Engineering apprenticeship to practice distributed DataFrame processing.

## Deliverables

| File | What it is |
|---|---|
| [`main.py`](main.py) | Single entry point - creates the SparkSession and runs the full pipeline |
| [`src/pipelines/movie_pipeline.py`](src/pipelines/movie_pipeline.py) | Orchestrates fetch -> clean -> analyze -> visualize |
| [`src/ingestion/api.py`](src/ingestion/api.py) | Fetches raw data from the TMDb API, with retries, timeouts, and payload validation |
| [`src/transformations/cleaning.py`](src/transformations/cleaning.py) | Cleans and transforms the raw data using PySpark's DataFrame API |
| [`src/analysis/kpis.py`](src/analysis/kpis.py) | Ranking, search, and aggregation logic |
| [`src/visualization/visualize.py`](src/visualization/visualize.py) | Chart generation, saved to `outputs/` |
| [`notebooks/analysis.ipynb`](notebooks/analysis.ipynb) | Full narrative walkthrough - KPIs, search results, and interpreted insights - start here if you only open one file |
| [`docs/architecture.md`](docs/architecture.md) | Pipeline flow diagram, Spark vs. pandas design differences, error handling and logging strategy |
| [`docs/adr/`](docs/adr/) | Standalone records for the significant technical decisions, including one carried over unchanged from the pandas version and one that supersedes it |
| [`outputs/`](outputs/) | The 5 generated chart PNGs |
| [`tests/test_cleaning.py`](tests/test_cleaning.py) | Cleaning logic and cleaned-DataFrame contract validation |
| [`tests/test_api_kpis_visualization.py`](tests/test_api_kpis_visualization.py) | Ingestion, KPI/search/aggregation, visualization, and frozen-output regression tests |
| [`tests/fixtures/frozen_dataset_spark_outputs.json`](tests/fixtures/frozen_dataset_spark_outputs.json) | Frozen Spark output baseline the regression test compares against |

## Project Structure

```
tmdb-movies-analysis-spark/
├── main.py                       # Pipeline entry point - creates SparkSession, runs the whole thing
├── requirements.txt               # Pinned dependencies, including PySpark
├── .env.example                   # Template for the required API key
├── src/
│   ├── ingestion/
│   │   └── api.py                 # Fetches raw data from the TMDb API
│   ├── transformations/
│   │   └── cleaning.py            # Cleans and transforms the raw data (PySpark)
│   ├── analysis/
│   │   └── kpis.py                # Ranking, search, and aggregation logic (PySpark)
│   ├── visualization/
│   │   └── visualize.py           # Chart generation, saved to outputs/ (pandas + Matplotlib)
│   ├── pipelines/
│   │   └── movie_pipeline.py      # Orchestrates the full pipeline end to end
│   ├── monitoring/
│   │   └── logging_config.py      # Centralized logging setup
│   └── utils/
│       └── constants.py           # Shared constants (e.g. MOVIE_IDS)
├── tests/
│   ├── conftest.py
│   ├── test_cleaning.py           # Cleaning logic + cleaned-contract validation
│   ├── test_api_kpis_visualization.py  # Ingestion, KPIs, charts, frozen regression
│   └── fixtures/
│       └── frozen_dataset_spark_outputs.json  # Frozen Spark output baseline
├── data/
│   ├── raw/                       # Raw API output (gitignored)
│   └── processed/                 # Cleaned dataset, saved as Parquet (gitignored)
├── outputs/                        # Saved chart PNGs
├── notebooks/
│   └── analysis.ipynb              # Narrative walkthrough - imports from src/, doesn't redefine logic
├── docs/
│   ├── architecture.md             # Design decisions and rationale
│   └── adr/                        # Architecture Decision Records
└── logs/
    └── pipeline.log                 # Runtime log (gitignored)
```

## How to Run

1. Clone this repo
2. Create a `.env` file (see `.env.example`) with your own TMDb API key
3. Install Java 17 and Apache Spark (see [architecture.md](docs/architecture.md) for the Windows-specific setup notes: `JAVA_HOME`, `SPARK_HOME`, `HADOOP_HOME`, and `winutils.exe`)
4. `pip install -r requirements.txt`
5. Run the full pipeline with one command:
   ```
   python main.py
   ```
   Useful flags:
   - `python main.py --skip-fetch` - reuse existing raw data instead of hitting the API again
   - `python main.py --show-charts` - display charts interactively as they're generated
6. Run the test suite:
   ```
   python -m pytest tests/ -v
   ```
7. Open `notebooks/analysis.ipynb` for the narrative walkthrough with commentary and insights

## Methodology

**Data Extraction:** Fetches 19 movie IDs from the TMDb API via `src/ingestion/api.py`, using `append_to_response=credits` to retrieve cast/crew data in the same request as movie details. Every request carries an explicit timeout and is checked against the API's actual HTTP status code, with the three failure modes handled differently: a 404 returns immediately (retrying can never make an invalid ID valid), while transient 5xx errors and network failures are retried with linear backoff that scales with the attempt number. Payloads that come back with a 200 are still not trusted - a lightweight contract check rejects any record missing `id`, `title`, `release_date`, `genres`, `credits`, `budget`, or `revenue`, so a malformed response fails at the boundary instead of surfacing as a confusing null halfway through cleaning. Individually invalid IDs are skipped and logged rather than aborting the run: ID `0` is invalid and correctly excluded, leaving 18 movies. If *every* ID fails, `fetch_all_movies()` raises instead of writing an empty raw JSON file, so the pipeline stops at the real cause. All requests share one `requests.Session` to keep TCP connections alive across calls - a small win at 19 requests, but the right default once call volume grows.

**Data Cleaning:** `src/transformations/cleaning.py` uses PySpark's DataFrame API throughout. Spark reads the raw JSON natively, automatically inferring a nested schema (structs and arrays) rather than leaving nested data as raw Python dicts. Extraction functions use Spark's `transform`, `filter`, `array_sort`, and `concat_ws` to convert nested structs into flat, alphabetically-sorted pipe-separated strings - sorting specifically prevents the same real-world combination (e.g. genres) from being treated as multiple different categories depending on API response order. Cast names are ordered by TMDb's billing order rather than sorted alphabetically, since billing order carries real meaning. Data types are cast explicitly, unrealistic zero-values are replaced with null using `F.when().otherwise()` (since Spark DataFrames are immutable and cannot be updated in place the way pandas' `.loc[]` can), and the dataset is filtered to released movies only. The cleaned result is saved as Parquet rather than CSV, preserving the schema's types (see [ADR-007](docs/adr/007-parquet-over-csv-for-spark-output.md)).

Before that write happens, `validate_cleaned_dataframe()` checks the final DataFrame against the pipeline's contract: the exact `FINAL_COLUMNS` list in order, non-null `id` and `title`, no duplicate ids, and value ranges (non-negative `vote_count`/`budget_musd`/`revenue_musd`/`runtime`, `vote_average` within 0-10). The distinction that matters here is between a *missing* value and a *broken* one. A null `budget_musd`, `revenue_musd`, or `runtime` is legitimate - it means TMDb reported 0, which cleaning deliberately converts to null because 0 means "unknown", not "free" or "instant". A negative budget or a duplicate id is a structural violation with no valid reading, so validation raises and the bad data never reaches Parquet.

**KPI Analysis:** `src/analysis/kpis.py` provides a reusable `rank_movies()` function used for all ranking queries (using Spark's `orderBy`/`limit` rather than pandas' `sort_values`/`head`). The requested metric is the primary sort key, with movie `id` ascending as a secondary key, so tied rows rank deterministically instead of falling back on whatever order Spark's partitioning happened to produce - a distributed engine gives no stable "input order" to rely on the way a single-threaded pandas sort effectively does.

`search_movies()` supports any combination of genre/cast/director filters via chained `.filter()` calls - Spark's lazy evaluation combines these into one efficient pass rather than scanning the data once per filter. Matching is case-insensitive and built entirely from native Spark expressions (`F.lower()` plus `contains()`), never a Python UDF, so the comparison stays inside the JVM and remains optimizable. Multiple requested genres combine with AND semantics.

Aggregations use `groupBy().agg()` with Spark's aggregate functions, with two deliberate refinements. `director_success()` splits co-directed films with `split()` + `explode()` before grouping, so a film directed by two people credits each director fully rather than treating "Anthony Russo|Joe Russo" as a third, separate director. `franchise_success()` counts only movies that actually belong to a collection, filtering out null and blank `belongs_to_collection` values before grouping - without that filter, every standalone movie collapses into a single unnamed "franchise" that then competes on the franchise leaderboard. Standalone titles are still fully represented in `compare_franchise_vs_standalone()`, which is the function whose entire purpose is holding the two groups side by side. The one other exception is median ROI, computed via `percentile_approx` since Spark has no cheap exact-median function for potentially distributed data.

**Visualization:** `src/visualization/visualize.py` is reused largely unchanged from the pandas version, since Matplotlib has no native support for Spark DataFrames. The one deliberate Spark-to-pandas conversion point in the whole pipeline happens in `movie_pipeline.py`, immediately before charts are generated - not hidden inside the plotting functions themselves. Generates and saves all 5 required charts (Revenue vs. Budget, ROI by Genre, Popularity vs. Rating, Yearly Box Office Trends, Franchise vs. Standalone comparison) to `outputs/` as PNG files.

**Orchestration:** `src/pipelines/movie_pipeline.py` ties fetch -> clean -> analyze -> visualize together, taking an active `SparkSession` as a parameter rather than creating one internally, so the same pipeline logic could in principle be triggered from a notebook or an orchestration tool without duplicating this sequence. `main.py` is a thin entry point: it creates the SparkSession, parses CLI flags, and calls `run_pipeline()`, stopping the Spark session in a `finally` block regardless of success or failure.

**Testing:** The suite runs against a local `SparkSession` fixture and covers five categories:

- **Ingestion** - 404 handling, retry-then-succeed on timeouts and transient HTTP errors, rejection of payloads missing required fields, skipping individually invalid IDs, and raising when zero usable records are fetched. Network calls are monkeypatched, so no test touches the live TMDb API.
- **Cleaning and data contract** - the extraction functions most prone to real bugs (cast/director parsing, bad-row removal), plus `validate_cleaned_dataframe()` tested one violated rule at a time. These DataFrames declare an explicit schema rather than relying on inference, since Spark cannot infer a type for an all-null column and would otherwise fail before the validation logic is ever reached.
- **KPI, search, and aggregation** - financial columns with null budgets, ranking filters, deterministic `id` tie-breaking, case-insensitive search including null cast/director, co-director splitting, and the exclusion of standalone/null collections from franchise results.
- **Visualization smoke tests** - all 5 chart functions run end to end against a temp directory and are asserted to write real PNG files.
- **Frozen-output regression** - `test_frozen_spark_baseline_outputs_regression` replays the deterministic cleaning and KPI path over the locked 18-movie raw pull and compares it to `tests/fixtures/frozen_dataset_spark_outputs.json`.

That fixture is a **Spark regression baseline, not a Pandas-to-Spark parity check**: every expected value was captured from an earlier run of this same Spark pipeline. It proves results have not drifted; it does not independently prove they are correct. Volatile fields (`popularity`, `vote_count`) are excluded so the baseline stays stable. A genuine cross-engine parity test, built from independently validated pandas outputs, is a separate piece of work not yet in this repo.

Because `data/raw/` is gitignored, that one regression test depends on the raw pull existing locally and skips with an explanatory message where it doesn't - including CI, which therefore runs the other 32. Current verified local result with the raw data present: **33 passed**.

**Logging:** All modules use a centralized logger (`src/monitoring/logging_config.py`) writing to both console and `logs/pipeline.log`, with explicit UTF-8 encoding to avoid crashes on non-ASCII text.

## Key Insights

- Budget and revenue show only a loose positive relationship - Avatar (~237M budget) earned nearly as much as Avengers: Endgame (~356M budget) despite a smaller budget, suggesting factors beyond spend (franchise strength, timing, audience appeal) drive returns.
- ROI varies significantly by genre, though several genres in this dataset are represented by only 1-2 movies, limiting how generalizable these patterns are.
- Popularity and rating show a loose positive relationship but are not strictly correlated.
- Standalone movies outperformed franchise movies on revenue, ROI, popularity, and rating in this dataset - based on only 2 standalone movies vs. 16 franchise movies, so this should not be read as a general claim about franchises vs. standalone films. Those 2 standalone titles are counted here, in the franchise-vs-standalone comparison, but are correctly absent from the franchise leaderboard below - they belong to no collection, so they form no franchise.
- Among actual collections, The Avengers Collection leads on total revenue (4 movies, ~7,776M), followed by the Star Wars Collection (~3,403M) and the Jurassic Park Collection (~2,982M). James Cameron leads among directors by total revenue (Titanic + Avatar).
- Two of the brief's specified search queries (Bruce Willis films, Uma Thurman/Tarantino films) returned no results - verified as correct, since neither actor/director appears in this project's fixed 19-movie dataset.

## Limitations

This analysis is based on a small, fixed set of 18 major blockbuster films specified by the project brief. Findings around genre, franchise vs. standalone performance, and yearly trends should be read as illustrative of the method rather than generalizable conclusions, given the limited and non-random sample. Additionally, this dataset's small size (18 rows) means Spark's distributed processing provides no real performance advantage over pandas here - the value of this version is in practicing Spark's DataFrame API and lazy evaluation model, not in solving a genuine big-data problem (see [architecture.md](docs/architecture.md) for a full discussion of this trade-off).

## Tools

Python, PySpark, pandas (for chart rendering only), Matplotlib, Requests, python-dotenv, Jupyter, pytest, Apache Spark 4.2.0, Java 17 (Eclipse Temurin). Pipeline scripts run in a standard pip-managed Python environment on Windows; the analysis notebook was developed and run using Jupyter within VS Code.