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