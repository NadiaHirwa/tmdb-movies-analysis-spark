"""
test_cleaning.py

Unit tests for the trickiest, most bug-prone functions in
transformations/cleaning.py - specifically the ones where real bugs
were caught and fixed during development of the pandas version of
this project, now re-verified against the PySpark rewrite.
"""

import re
from datetime import date

import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from transformations.cleaning import (
    FINAL_COLUMNS,
    extract_director_column,
    extract_names_column,
    remove_bad_rows,
    validate_cleaned_dataframe,
)

# The dtypes clean_dataframe() actually produces, verified against its output
# schema. Declaring them explicitly is what lets the validation tests below hold
# whole columns of nulls: Spark cannot infer a type for an all-null column and
# fails with CANNOT_DETERMINE_TYPE before validate_cleaned_dataframe() ever runs.
CLEANED_SCHEMA = StructType(
    [
        StructField("id", LongType()),
        StructField("title", StringType()),
        StructField("tagline", StringType()),
        StructField("release_date", DateType()),
        StructField("genres", StringType()),
        StructField("belongs_to_collection", StringType()),
        StructField("original_language", StringType()),
        StructField("budget_musd", DoubleType()),
        StructField("revenue_musd", DoubleType()),
        StructField("production_companies", StringType()),
        StructField("production_countries", StringType()),
        StructField("vote_count", LongType()),
        StructField("vote_average", DoubleType()),
        StructField("popularity", DoubleType()),
        StructField("runtime", LongType()),
        StructField("overview", StringType()),
        StructField("spoken_languages", StringType()),
        StructField("poster_path", StringType()),
        StructField("cast", StringType()),
        StructField("cast_size", IntegerType()),
        StructField("director", StringType()),
        StructField("crew_size", IntegerType()),
    ]
)


def cleaned_row(**values):
    """
    Build one CLEANED_SCHEMA row, defaulting every unspecified column to null.

    Rows are emitted as positional tuples ordered by FINAL_COLUMNS, the same list
    that orders CLEANED_SCHEMA, so a column can never silently land in the wrong
    field the way keyword Rows can when paired with an explicit schema.
    """
    unknown = sorted(set(values) - set(FINAL_COLUMNS))
    assert not unknown, f"Not columns of the cleaned contract: {unknown}"
    return tuple(values.get(column) for column in FINAL_COLUMNS)


def test_cleaned_schema_matches_the_final_column_contract():
    """Guard the fixture schema itself, so drift in FINAL_COLUMNS surfaces here."""
    assert CLEANED_SCHEMA.fieldNames() == FINAL_COLUMNS


@pytest.fixture(scope="module")
def spark():
    """A local SparkSession shared by all tests in this file."""
    session = SparkSession.builder.appName("TestCleaning").master("local[1]").getOrCreate()
    yield session
    session.stop()


def test_extract_names_column_sorts_consistently_regardless_of_input_order(spark):
    """
    Two rows with the same names in a different order must produce
    an identical output string. This is the exact bug found in the
    genres and production_countries columns during development of
    the pandas version, where 'Action|Adventure' and
    'Adventure|Action' were being treated as two different categories.
    """
    df = spark.createDataFrame([
        Row(genres=[Row(name="Action"), Row(name="Adventure"), Row(name="Science Fiction")]),
        Row(genres=[Row(name="Science Fiction"), Row(name="Action"), Row(name="Adventure")]),
    ])
    result = extract_names_column(df, "genres")
    values = [row["genres"] for row in result.collect()]

    assert values[0] == values[1]
    assert values[0] == "Action|Adventure|Science Fiction"


def test_extract_names_column_handles_empty_array(spark):
    """An empty array should produce null, not an empty string or an error."""
    df = spark.createDataFrame([Row(genres=[])], schema="genres array<struct<name:string>>")
    result = extract_names_column(df, "genres")
    assert result.collect()[0]["genres"] is None


def test_extract_director_column_finds_multiple_directors_and_sorts_them(spark):
    """
    Co-directed movies (e.g. the Russo brothers on the Avengers films)
    must have both directors' names joined and sorted alphabetically,
    regardless of their order or position within the crew list.
    """
    schema = "credits struct<cast: array<struct<name:string>>, crew: array<struct<name:string, job:string>>>"
    crew = [
        Row(name="Some Producer", job="Producer"),
        Row(name="Joe Russo", job="Director"),
        Row(name="Some Editor", job="Editor"),
        Row(name="Anthony Russo", job="Director"),
    ]
    df = spark.createDataFrame([Row(credits=Row(cast=[], crew=crew))], schema=schema)
    result = extract_director_column(df)
    assert result.collect()[0]["director"] == "Anthony Russo|Joe Russo"


def test_extract_director_column_ignores_non_director_jobs(spark):
    """Only crew members with job == 'Director' should be included."""
    schema = "credits struct<cast: array<struct<name:string>>, crew: array<struct<name:string, job:string>>>"
    crew = [
        Row(name="James Cameron", job="Producer"),
        Row(name="Someone Else", job="Writer"),
    ]
    df = spark.createDataFrame([Row(credits=Row(cast=[], crew=crew))], schema=schema)
    result = extract_director_column(df)
    assert result.collect()[0]["director"] is None


def test_remove_bad_rows_drops_duplicate_ids(spark):
    """Duplicate movie IDs must actually be removed."""
    df = spark.createDataFrame([
        Row(id=1, title="Movie A"),
        Row(id=2, title="Movie B"),
        Row(id=2, title="Movie B Duplicate"),
        Row(id=3, title="Movie C"),
    ])
    result = remove_bad_rows(df)
    ids = sorted(row["id"] for row in result.collect())
    assert ids == [1, 2, 3]


def test_remove_bad_rows_drops_missing_id_or_title(spark):
    """Rows with a missing id or a missing title must be dropped entirely."""
    df = spark.createDataFrame(
        [
            Row(id=1, title="Movie A"),
            Row(id=None, title="Movie B"),
            Row(id=3, title=None),
        ],
        schema="id long, title string",
    )
    result = remove_bad_rows(df)
    ids = [row["id"] for row in result.collect()]
    assert ids == [1]


def test_validate_cleaned_dataframe_accepts_valid_rows_with_missing_business_values(spark):
    """Unknown budget/revenue/runtime are legitimate nulls, not contract breaches."""
    df = spark.createDataFrame(
        [
            cleaned_row(
                id=101,
                title="Valid Movie",
                tagline="A tagline",
                release_date=date(2020, 1, 1),
                genres="Action|Drama",
                belongs_to_collection=None,
                original_language="en",
                budget_musd=None,
                revenue_musd=None,
                production_companies="Studio",
                production_countries="US",
                vote_count=150,
                vote_average=8.4,
                popularity=42.5,
                runtime=None,
                overview="A movie",
                spoken_languages="English",
                poster_path="/poster.jpg",
                cast="Actor One|Actor Two",
                cast_size=2,
                director="Jane Director",
                crew_size=5,
            )
        ],
        schema=CLEANED_SCHEMA,
    )

    assert df.columns == list(FINAL_COLUMNS)
    validate_cleaned_dataframe(df)


@pytest.mark.parametrize(
    "violation, expected_message",
    [
        ({"id": None}, "id is required and non-null"),
        ({"title": None}, "title is required and non-null"),
        ({"vote_count": -1}, "vote_count must be non-negative when present"),
        ({"vote_average": 11.0}, "vote_average must be between 0 and 10 when present"),
        ({"budget_musd": -1.0}, "budget_musd must be non-negative when present"),
        ({"revenue_musd": -1.0}, "revenue_musd must be non-negative when present"),
        ({"runtime": -5}, "runtime must be non-negative when present"),
    ],
)
def test_validate_cleaned_dataframe_rejects_contract_violations(spark, violation, expected_message):
    """
    One violated rule per case, so a failure names the rule that broke rather
    than any of eight possibilities matching one loose regex.
    """
    valid = {"id": 101, "title": "Valid Movie", "vote_count": 10, "vote_average": 8.5}
    df = spark.createDataFrame([cleaned_row(**{**valid, **violation})], schema=CLEANED_SCHEMA)

    with pytest.raises(ValueError, match=re.escape(expected_message)):
        validate_cleaned_dataframe(df)


def test_validate_cleaned_dataframe_rejects_duplicate_ids(spark):
    """remove_bad_rows() drops duplicate ids, so any survivor is a contract breach."""
    df = spark.createDataFrame(
        [
            cleaned_row(id=101, title="First Movie", vote_count=10, vote_average=8.5),
            cleaned_row(id=101, title="Duplicate Id", vote_count=20, vote_average=7.5),
        ],
        schema=CLEANED_SCHEMA,
    )

    with pytest.raises(ValueError, match="Duplicate ids"):
        validate_cleaned_dataframe(df)


def test_validate_cleaned_dataframe_rejects_wrong_column_contract(spark):
    """A missing final column must fail before any per-column rule is evaluated."""
    df = spark.createDataFrame(
        [cleaned_row(id=101, title="Valid Movie", vote_count=10, vote_average=8.5)],
        schema=CLEANED_SCHEMA,
    ).drop("crew_size")

    with pytest.raises(ValueError, match="column contract mismatch"):
        validate_cleaned_dataframe(df)