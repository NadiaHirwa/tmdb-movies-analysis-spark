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