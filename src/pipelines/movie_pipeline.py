"""
movie_pipeline.py

Orchestrates the full TMDB movie analysis pipeline end to end:
fetch -> clean -> analyze -> visualize. Kept separate from main.py so
the same pipeline logic can be triggered from a script, a notebook,
or (in principle) an Airflow DAG, without duplicating this sequence.
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from pyspark.sql import SparkSession

from monitoring.logging_config import get_logger
from ingestion.api import run as run_ingestion
from transformations.cleaning import load_raw_json, clean_dataframe, save_clean_data
from analysis.kpis import (
    add_financial_columns,
    add_franchise_status,
    rank_movies,
    compare_franchise_vs_standalone,
    franchise_success,
    director_success,
)
from visualization.visualize import generate_all_charts

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def ensure_directories() -> None:
    """
    Explicitly create every directory the pipeline writes to, before
    any pipeline stage runs. Each module also creates its own output
    directory defensively at write-time, but doing it upfront here
    makes it immediately obvious that a fresh clone of this repo
    will run correctly without any manual setup.
    """
    for folder in ["data/raw", "data/processed", "outputs", "logs"]:
        (PROJECT_ROOT / folder).mkdir(parents=True, exist_ok=True)


def run_pipeline(spark: SparkSession, skip_fetch: bool = False, show_charts: bool = False) -> None:
    """
    Run the full pipeline end to end: fetch -> clean -> analyze -> visualize.

    Parameters
    ----------
    spark : SparkSession
        The active Spark session, created by the caller (main.py or
        a notebook), rather than created inside this function - so
        the same session can be reused/inspected by the caller if
        needed, and tests can supply their own local session.
    skip_fetch : bool
        If True, skip the API fetch step and reuse the existing raw
        JSON file already saved on disk.
    show_charts : bool
        If True, display each chart interactively as it's generated.
    """
    ensure_directories()
    logger.info("Pipeline started.")

    if not skip_fetch:
        logger.info("Step 1/4: Fetching data from TMDb API")
        run_ingestion()
    else:
        logger.info("Step 1/4: Skipped (--skip-fetch set, reusing existing raw data)")

    logger.info("Step 2/4: Cleaning data")
    df = load_raw_json(spark)
    df = clean_dataframe(df)
    save_clean_data(df)

    logger.info("Step 3/4: Computing KPIs")
    df = add_financial_columns(df)
    df = add_franchise_status(df)

    top_revenue = rank_movies(df, by="revenue_musd", n=5)
    comparison = compare_franchise_vs_standalone(df)
    franchises = franchise_success(df)
    directors = director_success(df)

    # Spark DataFrames don't have a lightweight .to_string() the way
    # pandas does; converting these small, already-aggregated results
    # to pandas purely for readable logging is safe and cheap here.
    logger.info("Top 5 movies by revenue:\n%s", top_revenue.select("title", "revenue_musd").toPandas().to_string())
    logger.info("Franchise vs Standalone comparison:\n%s", comparison.toPandas().to_string())
    logger.info("Top 5 franchises by total revenue:\n%s", franchises.limit(5).toPandas().to_string())
    logger.info("Top 5 directors by total revenue:\n%s", directors.limit(5).toPandas().to_string())

    logger.info("Step 4/4: Generating visualizations")
    # visualize.py expects pandas DataFrames (see its module docstring) -
    # this is the one deliberate Spark-to-pandas conversion point in
    # the whole pipeline, done here at the boundary, not hidden inside
    # the plotting functions themselves.
    generate_all_charts(df.toPandas(), comparison.toPandas(), show=show_charts)

    logger.info("Pipeline completed successfully.")