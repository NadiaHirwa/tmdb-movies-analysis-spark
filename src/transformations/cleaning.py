"""
cleaning.py

Loads the raw TMDb JSON pull and transforms it into a clean,
analysis-ready Spark DataFrame.
"""

import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parent.parent))
from monitoring.logging_config import get_logger

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw" / "movies_raw.json"

IRRELEVANT_COLUMNS = ["adult", "imdb_id", "original_title", "video", "homepage"]

LIST_COLUMNS = ["genres", "production_companies", "production_countries", "spoken_languages"]


def load_raw_json(spark: SparkSession, path: str = str(RAW_DATA_PATH)) -> DataFrame:
    """
    Load the raw movie JSON directly into a Spark DataFrame.

    Spark reads JSON natively and infers a schema automatically, so
    there's no separate "load into a Python list, then build a
    DataFrame" step the way there was in the pandas version.
    """
    df = spark.read.option("multiline", "true").json(path)
    logger.info("Loaded raw DataFrame with %d rows", df.count())
    return df


def drop_irrelevant_columns(df: DataFrame, columns: list[str] = IRRELEVANT_COLUMNS) -> DataFrame:
    """Drop columns that add no analytical value for this project."""
    existing = [c for c in columns if c in df.columns]
    df = df.drop(*existing)
    logger.info("Dropped columns: %s", existing)
    return df


def extract_collection_name(df: DataFrame) -> DataFrame:
    """
    belongs_to_collection is a single struct (or null); extract just
    its name field. Spark already parsed the nested JSON into a typed
    struct, so this is direct dot-notation access - no isinstance()
    check needed, since a null struct's .name is simply null.
    """
    return df.withColumn("belongs_to_collection", F.col("belongs_to_collection.name"))


def extract_names_column(df: DataFrame, column: str, key: str = "name", sep: str = "|") -> DataFrame:
    """
    Convert an array<struct> column (e.g. genres) into a single,
    alphabetically sorted, pipe-separated string column.

    Sorting ensures the same combination of values always produces
    an identical string regardless of the order the API returned
    them in - the same real bug this guarded against in the pandas
    version.
    """
    return df.withColumn(
        column,
        F.when(
            F.size(F.col(column)) > 0,
            F.concat_ws(sep, F.array_sort(F.transform(F.col(column), lambda x: x[key]))),
        ).otherwise(F.lit(None)),
    )


def extract_list_columns(df: DataFrame, columns: list[str] = LIST_COLUMNS) -> DataFrame:
    """Apply extract_names_column() to each column in `columns`."""
    for col_name in columns:
        if col_name in df.columns:
            df = extract_names_column(df, col_name)
    logger.info("Extracted names from columns: %s", columns)
    return df


def extract_cast_column(df: DataFrame, sep: str = "|") -> DataFrame:
    """
    Extract cast names from credits.cast, ordered by TMDb's billing
    order (order=0 is top-billed) - deliberately NOT alphabetically
    sorted, since billing order carries real meaning.

    Sorts a temporary array<struct<order, name>> (Spark sorts structs
    by their first field by default), then discards the order field,
    keeping just the ordered names.
    """
    cast_sorted = F.array_sort(
        F.transform(
            F.col("credits.cast"),
            lambda x: F.struct(x["order"].alias("order"), x["name"].alias("name")),
        )
    )
    df = df.withColumn("cast", F.concat_ws(sep, F.transform(cast_sorted, lambda x: x["name"])))
    df = df.withColumn("cast_size", F.size(F.col("credits.cast")))
    return df


def extract_director_column(df: DataFrame, sep: str = "|") -> DataFrame:
    """Search credits.crew for anyone with job == 'Director'."""
    directors = F.filter(F.col("credits.crew"), lambda x: x["job"] == "Director")
    director_names = F.array_sort(F.transform(directors, lambda x: x["name"]))
    df = df.withColumn("director", F.concat_ws(sep, director_names))
    df = df.withColumn("crew_size", F.size(F.col("credits.crew")))
    return df


def extract_credits_columns(df: DataFrame) -> DataFrame:
    """Break the nested 'credits' struct into cast, cast_size, director, crew_size."""
    if "credits" not in df.columns:
        logger.warning("'credits' column not found - skipping credit extraction")
        return df

    df = extract_cast_column(df)
    df = extract_director_column(df)
    df = df.drop("credits")
    logger.info("Extracted cast, cast_size, director, crew_size from credits")
    return df


def convert_dtypes(df: DataFrame) -> DataFrame:
    """
    Cast budget/id/popularity to numeric types and release_date to a
    proper date type.

    Spark already inferred numeric types when reading the JSON (see
    printSchema() earlier), but casting explicitly here is still
    worth doing: it's cheap insurance if a future API response ever
    returns a numeric field as a string, and .cast() mirrors
    pd.to_numeric(..., errors="coerce") directly - a value that
    can't be cast becomes null instead of crashing the pipeline.
    """
    df = df.withColumn("budget", F.col("budget").cast("double"))
    df = df.withColumn("id", F.col("id").cast("long"))
    df = df.withColumn("popularity", F.col("popularity").cast("double"))
    df = df.withColumn("release_date", F.to_date(F.col("release_date"), "yyyy-MM-dd"))
    logger.info("Converted dtypes for budget, id, popularity, release_date")
    return df


def fix_unrealistic_values(df: DataFrame) -> DataFrame:
    """
    Replace 0 with null for budget/revenue/runtime (0 means unknown,
    not free/instant), convert budget/revenue to million-USD columns,
    and set vote_average to null wherever vote_count is 0 (a rating
    with zero votes behind it isn't meaningful).
    """
    for col_name in ["budget", "revenue", "runtime"]:
        df = df.withColumn(
            col_name,
            F.when(F.col(col_name) == 0, None).otherwise(F.col(col_name)),
        )

    df = df.withColumn("budget_musd", F.col("budget") / 1_000_000)
    df = df.withColumn("revenue_musd", F.col("revenue") / 1_000_000)

    zero_vote_count = df.filter(F.col("vote_count") == 0).count()
    df = df.withColumn(
        "vote_average",
        F.when(F.col("vote_count") == 0, None).otherwise(F.col("vote_average")),
    )
    logger.info("Set vote_average to NaN for %d rows with vote_count == 0", zero_vote_count)

    for col_name in ["overview", "tagline"]:
        df = df.withColumn(
            col_name,
            F.when(F.col(col_name) == "No Data", None).otherwise(F.col(col_name)),
        )

    return df


def remove_bad_rows(df: DataFrame) -> DataFrame:
    """
    Actually drop duplicate movie IDs and rows with an unknown id/title,
    rather than just reporting their counts.
    """
    before = df.count()
    df = df.dropDuplicates(subset=["id"])
    df = df.dropna(subset=["id", "title"])
    after = df.count()
    logger.info("Removed %d bad rows (duplicates / missing id or title)", before - after)
    return df


def filter_sparse_rows(df: DataFrame, min_non_null: int = 10) -> DataFrame:
    """
    Keep only rows where at least `min_non_null` columns are non-null.

    Spark has no row-wise equivalent of pandas' df.notnull().sum(axis=1),
    since Spark operations work column-by-column across all rows, never
    "across" a single row. Instead, we build the per-row non-null count
    by summing, column by column, a 1-or-0 flag for each column.
    """
    before = df.count()

    non_null_count = sum(
        F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in df.columns
    )
    df = df.filter(non_null_count >= min_non_null)

    after = df.count()
    logger.info("Dropped %d sparse rows (fewer than %d non-null columns)", before - after, min_non_null)
    return df