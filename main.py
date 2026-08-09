"""
main.py

Single entry point for the entire TMDB movie analysis pipeline.
Creates the SparkSession and runs ingestion, cleaning, KPI
computation, and visualization in sequence, so the whole project
can be reproduced with one command:

    python main.py
    python main.py --skip-fetch     (reuse existing raw data, skip the API call)
    python main.py --show-charts    (display charts interactively as they're made)
"""

import sys
import argparse
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent / "src"))

from pyspark.sql import SparkSession

from monitoring.logging_config import get_logger
from pipelines.movie_pipeline import run_pipeline

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command-line flags for the pipeline runner."""
    parser = argparse.ArgumentParser(description="Run the TMDB movie analysis pipeline.")
    parser.add_argument(
        "--skip-fetch",
        action="store_true",
        help="Skip the API fetch step and reuse existing raw data.",
    )
    parser.add_argument(
        "--show-charts",
        action="store_true",
        help="Display charts interactively as they're generated.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    spark = SparkSession.builder.appName("TMDBMovieAnalysis").master("local[*]").getOrCreate()

    try:
        run_pipeline(spark, skip_fetch=args.skip_fetch, show_charts=args.show_charts)
    except Exception:
        logger.exception("Pipeline failed with an unhandled error.")
        sys.exit(1)
    finally:
        spark.stop()