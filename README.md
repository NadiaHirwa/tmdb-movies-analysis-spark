# TMDB Movie Data Analysis (Spark)

![Tests](https://github.com/NadiaHirwa/tmdb-movies-analysis-spark/actions/workflows/tests.yml/badge.svg)

A data engineering pipeline that fetches movie data from the TMDb API, cleans and transforms it with PySpark, calculates business KPIs, and visualizes key trends. This is a PySpark rewrite of an [earlier pandas-based version](https://github.com/NadiaHirwa/tmdb-movies-analysis) of the same project, built as part of a Data Engineering apprenticeship to practice distributed DataFrame processing.

## Deliverables

| File | What it is |
|---|---|
| [`main.py`](main.py) | Single entry point - creates the SparkSession and runs the full pipeline |
| [`src/pipelines/movie_pipeline.py`](src/pipelines/movie_pipeline.py) | Orchestrates fetch -> clean -> analyze -> visualize |
| [`src/ingestion/api.py`](src/ingestion/api.py) | Fetches raw data from the TMDb API (reused unchanged from the pandas version - see [architecture.md](docs/architecture.md)) |
| [`src/transformations/cleaning.py`](src/transformations/cleaning.py) | Cleans and transforms the raw data using PySpark's DataFrame API |
| [`src/analysis/kpis.py`](src/analysis/kpis.py) | Ranking, search, and aggregation logic |
| [`src/visualization/visualize.py`](src/visualization/visualize.py) | Chart generation, saved to `outputs/` |
| [`notebooks/analysis.ipynb`](notebooks/analysis.ipynb) | Full narrative walkthrough - KPIs, search results, and interpreted insights - start here if you only open one file |
| [`docs/architecture.md`](docs/architecture.md) | Pipeline flow diagram, Spark vs. pandas design differences, error handling and logging strategy |
| [`docs/adr/`](docs/adr/) | Standalone records for the significant technical decisions, including one carried over unchanged from the pandas version and one that supersedes it |
| [`outputs/`](outputs/) | The 5 generated chart PNGs |
| [`tests/`](tests/) | Unit tests for the cleaning logic |

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
│   └── test_cleaning.py
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

**Data Extraction:** Fetches 19 movie IDs from the TMDb API via `src/ingestion/api.py`, using `append_to_response=credits` to retrieve cast/crew data in the same request as movie details. Every request has an explicit timeout, is retried with linear backoff on transient failures, and is checked against the API's actual HTTP status code. One ID (`0`) is invalid and correctly excluded, leaving 18 movies. This module is reused unchanged from the pandas version, since data extraction has no dependency on the processing engine used downstream.

**Data Cleaning:** `src/transformations/cleaning.py` uses PySpark's DataFrame API throughout. Spark reads the raw JSON natively, automatically inferring a nested schema (structs and arrays) rather than leaving nested data as raw Python dicts. Extraction functions use Spark's `transform`, `filter`, `array_sort`, and `concat_ws` to convert nested structs into flat, alphabetically-sorted pipe-separated strings - sorting specifically prevents the same real-world combination (e.g. genres) from being treated as multiple different categories depending on API response order. Cast names are ordered by TMDb's billing order rather than sorted alphabetically, since billing order carries real meaning. Data types are cast explicitly, unrealistic zero-values are replaced with null using `F.when().otherwise()` (since Spark DataFrames are immutable and cannot be updated in place the way pandas' `.loc[]` can), and the dataset is filtered to released movies only. The cleaned result is saved as Parquet rather than CSV, preserving the schema's types (see [ADR-007](docs/adr/007-parquet-over-csv-for-spark-output.md)).

**KPI Analysis:** `src/analysis/kpis.py` provides a reusable `rank_movies()` function used for all ranking queries (using Spark's `orderBy`/`limit` rather than pandas' `sort_values`/`head`), and a generalized `search_movies()` function supporting any combination of genre/cast/director filters via chained `.filter()` calls - Spark's lazy evaluation combines these into one efficient pass rather than scanning the data once per filter. Franchise vs. standalone comparisons and franchise/director success rankings use `groupBy().agg()` with Spark's aggregate functions; the one exception is median ROI, computed via `percentile_approx` since Spark has no cheap exact-median function for potentially distributed data.

**Visualization:** `src/visualization/visualize.py` is reused largely unchanged from the pandas version, since Matplotlib has no native support for Spark DataFrames. The one deliberate Spark-to-pandas conversion point in the whole pipeline happens in `movie_pipeline.py`, immediately before charts are generated - not hidden inside the plotting functions themselves. Generates and saves all 5 required charts (Revenue vs. Budget, ROI by Genre, Popularity vs. Rating, Yearly Box Office Trends, Franchise vs. Standalone comparison) to `outputs/` as PNG files.

**Orchestration:** `src/pipelines/movie_pipeline.py` ties fetch -> clean -> analyze -> visualize together, taking an active `SparkSession` as a parameter rather than creating one internally, so the same pipeline logic could in principle be triggered from a notebook or an orchestration tool without duplicating this sequence. `main.py` is a thin entry point: it creates the SparkSession, parses CLI flags, and calls `run_pipeline()`, stopping the Spark session in a `finally` block regardless of success or failure.

**Testing:** `tests/test_cleaning.py` covers the cleaning logic, adapted for Spark DataFrames and a local `SparkSession` fixture.

**Logging:** All modules use a centralized logger (`src/monitoring/logging_config.py`) writing to both console and `logs/pipeline.log`, with explicit UTF-8 encoding to avoid crashes on non-ASCII text.

## Key Insights

- Budget and revenue show only a loose positive relationship - Avatar (~237M budget) earned nearly as much as Avengers: Endgame (~356M budget) despite a smaller budget, suggesting factors beyond spend (franchise strength, timing, audience appeal) drive returns.
- ROI varies significantly by genre, though several genres in this dataset are represented by only 1-2 movies, limiting how generalizable these patterns are.
- Popularity and rating show a loose positive relationship but are not strictly correlated.
- Standalone movies outperformed franchise movies on revenue, ROI, popularity, and rating in this dataset - based on only 2 standalone movies vs. 16 franchise movies, so this should not be read as a general claim about franchises vs. standalone films.
- The Avengers Collection leads in total franchise revenue; James Cameron leads among directors by total revenue (Titanic + Avatar).
- Two of the brief's specified search queries (Bruce Willis films, Uma Thurman/Tarantino films) returned no results - verified as correct, since neither actor/director appears in this project's fixed 19-movie dataset.

## Limitations

This analysis is based on a small, fixed set of 18 major blockbuster films specified by the project brief. Findings around genre, franchise vs. standalone performance, and yearly trends should be read as illustrative of the method rather than generalizable conclusions, given the limited and non-random sample. Additionally, this dataset's small size (18 rows) means Spark's distributed processing provides no real performance advantage over pandas here - the value of this version is in practicing Spark's DataFrame API and lazy evaluation model, not in solving a genuine big-data problem (see [architecture.md](docs/architecture.md) for a full discussion of this trade-off).

## Tools

Python, PySpark, pandas (for chart rendering only), Matplotlib, Requests, python-dotenv, Jupyter, pytest, Apache Spark 4.2.0, Java 17 (Eclipse Temurin). Pipeline scripts run in a standard pip-managed Python environment on Windows; the analysis notebook was developed and run using Jupyter within VS Code.