"""
kpis.py

Reusable functions for ranking, searching, and aggregating the cleaned
movies dataset.
"""

import sys
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parent.parent))
from monitoring.logging_config import get_logger

logger = get_logger(__name__)

def add_financial_columns(df: DataFrame) -> DataFrame:
    """
    Add profit_musd (revenue - budget) and roi (revenue / budget)
    columns, both in million-USD terms, to support ranking and
    aggregation by financial performance.
    """
    df = df.withColumn("profit_musd", F.col("revenue_musd") - F.col("budget_musd"))
    df = df.withColumn("roi", F.col("revenue_musd") / F.col("budget_musd"))
    logger.info("Added profit_musd and roi columns")
    return df



def rank_movies(
    data: DataFrame,
    by: str,
    ascending: bool = False,
    filter_column: str | None = None,
    min_value: float | None = None,
    n: int = 10,
) -> DataFrame:
    """
    Return the top (or bottom) `n` movies ranked by a given column,
    with an optional minimum-value filter applied first.
    """
    if filter_column is not None:
        data = data.filter(F.col(filter_column) >= min_value)

    order_col = F.col(by).asc() if ascending else F.col(by).desc()
    return data.orderBy(order_col).limit(n)


def search_movies(
    df: DataFrame,
    genre_contains: str | list[str] | None = None,
    cast_contains: str | None = None,
    director_contains: str | None = None,
    sort_by: str | None = None,
    ascending: bool = True,
) -> DataFrame:
    """
    Filter movies by any combination of genre, cast member, and/or
    director substring matches, optionally sorted afterward.
    """
    if genre_contains is not None:
        genres_needed = [genre_contains] if isinstance(genre_contains, str) else genre_contains
        for genre in genres_needed:
            df = df.filter(F.col("genres").contains(genre))

    if cast_contains is not None:
        df = df.filter(F.col("cast").contains(cast_contains))

    if director_contains is not None:
        df = df.filter(F.col("director").contains(director_contains))

    logger.info("search_movies matched %d rows", df.count())

    if sort_by is not None:
        order_col = F.col(sort_by).asc() if ascending else F.col(sort_by).desc()
        df = df.orderBy(order_col)

    return df


def add_franchise_status(df: DataFrame) -> DataFrame:
    """Add a readable 'franchise_status' column: 'Franchise' or 'Standalone'."""
    return df.withColumn(
        "franchise_status",
        F.when(F.col("belongs_to_collection").isNotNull(), "Franchise").otherwise("Standalone"),
    )


def compare_franchise_vs_standalone(df: DataFrame) -> DataFrame:
    """
    Compare franchise vs. standalone movies across mean revenue,
    median ROI, mean budget, mean popularity, and mean rating.
    """
    return df.groupBy("franchise_status").agg(
        F.mean("revenue_musd").alias("mean_revenue"),
        F.expr("percentile_approx(roi, 0.5)").alias("median_roi"),
        F.mean("budget_musd").alias("mean_budget"),
        F.mean("popularity").alias("mean_popularity"),
        F.mean("vote_average").alias("mean_rating"),
    )


def franchise_success(df: DataFrame) -> DataFrame:
    """
    Summarize each franchise (belongs_to_collection group) by movie
    count, total/mean budget, total/mean revenue, and mean rating,
    ranked by total revenue.
    """
    summary = df.groupBy("belongs_to_collection").agg(
        F.count("id").alias("num_movies"),
        F.sum("budget_musd").alias("total_budget"),
        F.mean("budget_musd").alias("mean_budget"),
        F.sum("revenue_musd").alias("total_revenue"),
        F.mean("revenue_musd").alias("mean_revenue"),
        F.mean("vote_average").alias("mean_rating"),
    )
    return summary.orderBy(F.desc("total_revenue"))


def director_success(df: DataFrame) -> DataFrame:
    """
    Summarize each director by movie count, total revenue, and mean
    rating, ranked by total revenue.
    """
    summary = df.groupBy("director").agg(
        F.count("id").alias("num_movies"),
        F.sum("revenue_musd").alias("total_revenue"),
        F.mean("vote_average").alias("mean_rating"),
    )
    return summary.orderBy(F.desc("total_revenue"))