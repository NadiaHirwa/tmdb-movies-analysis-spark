"""
test_cleaning.py

Unit tests for the trickiest, most bug-prone functions in
transformations/cleaning.py - specifically the ones where real bugs
were caught and fixed during development of the pandas version of
this project, now re-verified against the PySpark rewrite.
"""

import pytest
from pyspark.sql import SparkSession, Row

from transformations.cleaning import extract_names_column, extract_director_column, remove_bad_rows


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