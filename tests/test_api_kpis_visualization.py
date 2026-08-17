import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from pyspark.sql import Row, SparkSession

from analysis import kpis
from ingestion import api as api_module
from transformations.cleaning import RAW_DATA_PATH, clean_dataframe, load_raw_json
from visualization import visualize

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.appName("TestKpisAndApi").master("local[1]").getOrCreate()
    yield session
    session.stop()


def test_fetch_movie_returns_none_for_404(monkeypatch):
    calls = []

    def fake_get(url, params=None, timeout=None):
        calls.append((url, params, timeout))
        return SimpleNamespace(status_code=404)

    monkeypatch.setattr(api_module.requests, "get", fake_get)
    monkeypatch.setattr(api_module, "API_KEY", "test-key")
    monkeypatch.setattr(api_module.time, "sleep", lambda *_args, **_kwargs: None)

    assert api_module.fetch_movie(9999, retries=3, backoff=1.0, timeout=5) is None
    assert len(calls) == 1


def test_fetch_movie_retries_after_timeout_then_succeeds(monkeypatch):
    attempts = {"count": 0}

    def fake_get(url, params=None, timeout=None):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise api_module.requests.exceptions.Timeout("slow response")

        response = SimpleNamespace(status_code=200)
        response.raise_for_status = lambda: None
        response.json = lambda: {
            "id": 42,
            "title": "Retry Success",
            "release_date": "2020-01-01",
            "genres": [{"name": "Action"}],
            "credits": {"cast": [], "crew": []},
            "budget": 10,
            "revenue": 20,
        }
        return response

    monkeypatch.setattr(api_module.requests, "get", fake_get)
    monkeypatch.setattr(api_module, "API_KEY", "test-key")
    monkeypatch.setattr(api_module.time, "sleep", lambda *_args, **_kwargs: None)

    movie = api_module.fetch_movie(42, retries=3, backoff=1.0, timeout=5)
    assert movie["title"] == "Retry Success"
    assert attempts["count"] == 2


def test_fetch_movie_retries_transient_http_errors(monkeypatch):
    attempts = {"count": 0}

    class FakeResponse:
        status_code = 500

        def raise_for_status(self):
            raise api_module.requests.exceptions.HTTPError("server error")

    def fake_get(url, params=None, timeout=None):
        attempts["count"] += 1
        if attempts["count"] == 1:
            return FakeResponse()

        response = SimpleNamespace(status_code=200)
        response.raise_for_status = lambda: None
        response.json = lambda: {
            "id": 7,
            "title": "Recovered",
            "release_date": "2021-02-02",
            "genres": [{"name": "Drama"}],
            "credits": {"cast": [], "crew": []},
            "budget": 30,
            "revenue": 60,
        }
        return response

    monkeypatch.setattr(api_module.requests, "get", fake_get)
    monkeypatch.setattr(api_module, "API_KEY", "test-key")
    monkeypatch.setattr(api_module.time, "sleep", lambda *_args, **_kwargs: None)

    movie = api_module.fetch_movie(7, retries=3, backoff=1.0, timeout=5)
    assert movie["title"] == "Recovered"
    assert attempts["count"] == 2


def test_fetch_movie_rejects_payload_missing_required_fields(monkeypatch):
    def fake_get(url, params=None, timeout=None):
        response = SimpleNamespace(status_code=200)
        response.raise_for_status = lambda: None
        response.json = lambda: {"id": 10, "title": "Incomplete"}
        return response

    monkeypatch.setattr(api_module.requests, "get", fake_get)
    monkeypatch.setattr(api_module, "API_KEY", "test-key")
    monkeypatch.setattr(api_module.time, "sleep", lambda *_args, **_kwargs: None)

    assert api_module.fetch_movie(10, retries=2, backoff=0.1, timeout=5) is None


def test_fetch_all_movies_skips_invalid_ids_but_returns_valid_records(monkeypatch):
    movie_map = {
        1: {"id": 1, "title": "Valid One", "release_date": "2020-01-01", "genres": [{"name": "Action"}], "credits": {"cast": [], "crew": []}, "budget": 100, "revenue": 200},
        2: None,
        3: {"id": 3, "title": "Valid Two", "release_date": "2021-02-02", "genres": [{"name": "Drama"}], "credits": {"cast": [], "crew": []}, "budget": 50, "revenue": 75},
    }

    def fake_fetch_movie(movie_id, retries=3, backoff=1.0, timeout=10, session=None, **kwargs):
        return movie_map.get(movie_id)

    monkeypatch.setattr(api_module, "fetch_movie", fake_fetch_movie)

    result = api_module.fetch_all_movies([1, 2, 3])
    assert [movie["id"] for movie in result] == [1, 3]


def test_fetch_all_movies_raises_when_zero_usable_records_are_fetched(monkeypatch):
    def fake_fetch_movie(movie_id, retries=3, backoff=1.0, timeout=10, session=None, **kwargs):
        return None

    monkeypatch.setattr(api_module, "fetch_movie", fake_fetch_movie)

    with pytest.raises(ValueError, match="Zero usable movie records were fetched"):
        api_module.fetch_all_movies([99, 100])


def test_add_financial_columns_handles_null_budget_and_calculates_metrics(spark):
    df = spark.createDataFrame(
        [
            Row(title="Movie A", revenue_musd=200.0, budget_musd=50.0),
            Row(title="Movie B", revenue_musd=80.0, budget_musd=None),
        ]
    )

    result = kpis.add_financial_columns(df).orderBy("title").collect()

    assert result[0]["profit_musd"] == pytest.approx(150.0)
    assert result[0]["roi"] == pytest.approx(4.0)
    assert result[1]["profit_musd"] is None
    assert result[1]["roi"] is None


def test_rank_movies_filters_by_min_budget_and_min_votes(spark):
    df = spark.createDataFrame(
        [
            Row(id=299534, title="Low Budget", budget_musd=10.0, revenue_musd=5.0, vote_count=100, vote_average=7.5),
            Row(id=19995, title="Mid Budget", budget_musd=100.0, revenue_musd=500.0, vote_count=5, vote_average=8.5),
            Row(id=140607, title="High Budget", budget_musd=200.0, revenue_musd=1000.0, vote_count=50, vote_average=9.0),
            Row(id=299536, title="Low Votes", budget_musd=300.0, revenue_musd=2000.0, vote_count=2, vote_average=8.0),
        ]
    )

    budget_ranked = kpis.rank_movies(df, by="revenue_musd", filter_column="budget_musd", min_value=100, n=10)
    vote_ranked = kpis.rank_movies(df, by="vote_average", filter_column="vote_count", min_value=10, n=10)

    budget_titles = [row["title"] for row in budget_ranked.collect()]
    vote_titles = [row["title"] for row in vote_ranked.collect()]

    assert budget_titles == ["Low Votes", "High Budget", "Mid Budget"]
    assert vote_titles == ["High Budget", "Low Budget"]


def test_rank_movies_tie_breaks_on_id_ascending(spark):
    df = spark.createDataFrame(
        [
            Row(id=200, title="Later Input", revenue_musd=900.0, budget_musd=300.0),
            Row(id=50, title="Earlier Input", revenue_musd=900.0, budget_musd=300.0),
            Row(id=10, title="Lower Revenue", revenue_musd=100.0, budget_musd=50.0),
        ]
    )

    ranked = kpis.rank_movies(df, by="revenue_musd", n=10)
    titles = [row["title"] for row in ranked.collect()]

    # Even though "Later Input" appears first in the source data, Spark should not
    # rely on incidental row order when two movies share the same metric value.
    assert titles == ["Earlier Input", "Later Input", "Lower Revenue"]


def test_search_movies_is_case_insensitive_and_handles_null_cast_or_director(spark):
    df = spark.createDataFrame(
        [
            Row(title="Action Movie", genres="Action|Science Fiction", cast="Tom Cruise|Emily Blunt", director="Christopher Nolan"),
            Row(title="Action Drama", genres="action|Drama", cast=None, director="christopher nolan"),
            Row(title="Sci Fi", genres="Science Fiction", cast="Emily Blunt", director=None),
            Row(title="Adventure Movie", genres="Adventure|Action", cast="Brad Pitt", director="Quentin Tarantino"),
        ]
    )

    mixed_case_genre_result = kpis.search_movies(df, genre_contains=["action", "drama"])
    cast_director_result = kpis.search_movies(df, cast_contains="tom cruise", director_contains="nolan")
    null_field_result = kpis.search_movies(df, cast_contains="emily blunt", director_contains="nolan")

    assert [row["title"] for row in mixed_case_genre_result.collect()] == ["Action Drama"]
    assert [row["title"] for row in cast_director_result.collect()] == ["Action Movie"]
    assert [row["title"] for row in null_field_result.collect()] == ["Action Movie"]


def test_search_movies_returns_zero_results_for_required_queries(spark):
    df = spark.createDataFrame(
        [
            Row(title="Action Movie", genres="Action|Science Fiction", cast="Tom Cruise|Emily Blunt", director="Christopher Nolan"),
            Row(title="Crime Film", genres="Drama|Crime", cast="Uma Thurman", director="David Fincher"),
            Row(title="Adventure Movie", genres="Adventure|Action", cast="Brad Pitt", director="Quentin Tarantino"),
        ]
    )

    bruce_result = kpis.search_movies(df, genre_contains=["Action", "Science Fiction"], cast_contains="Bruce Willis")
    uma_result = kpis.search_movies(df, cast_contains="Uma Thurman", director_contains="Quentin Tarantino")

    assert bruce_result.count() == 0
    assert uma_result.count() == 0


def test_franchise_and_director_aggregations_match_expected_summary_rows(spark):
    df = spark.createDataFrame(
        [
            Row(id=24428, title="Franchise A 1", belongs_to_collection="Franchise A", director="Director B", revenue_musd=100.0, budget_musd=50.0, popularity=10.0, vote_average=8.0),
            Row(id=99861, title="Franchise A 2", belongs_to_collection="Franchise A", director="Director B", revenue_musd=50.0, budget_musd=25.0, popularity=4.0, vote_average=6.0),
            Row(id=597, title="Standalone 1", belongs_to_collection=None, director="Director C", revenue_musd=80.0, budget_musd=20.0, popularity=9.0, vote_average=9.0),
            Row(id=284054, title="Standalone 2", belongs_to_collection=None, director="Director C", revenue_musd=40.0, budget_musd=10.0, popularity=3.0, vote_average=5.0),
        ]
    )

    df = kpis.add_financial_columns(df)
    df = kpis.add_franchise_status(df)
    comparison = kpis.compare_franchise_vs_standalone(df)
    franchise_summary = kpis.franchise_success(df)
    director_summary = kpis.director_success(df)

    comparison_rows = {row["franchise_status"]: row for row in comparison.collect()}
    assert comparison_rows["Franchise"]["mean_revenue"] == pytest.approx(75.0)
    assert comparison_rows["Franchise"]["mean_budget"] == pytest.approx(37.5)
    assert comparison_rows["Franchise"]["mean_popularity"] == pytest.approx(7.0)
    assert comparison_rows["Franchise"]["mean_rating"] == pytest.approx(7.0)
    assert comparison_rows["Franchise"]["median_roi"] == pytest.approx(2.0)

    assert comparison_rows["Standalone"]["mean_revenue"] == pytest.approx(60.0)
    assert comparison_rows["Standalone"]["mean_budget"] == pytest.approx(15.0)
    assert comparison_rows["Standalone"]["median_roi"] == pytest.approx(4.0)

    # Standalone titles stay in compare_franchise_vs_standalone() above; they must
    # not leak into the franchise leaderboard as an unnamed null "collection".
    franchise_collections = [row["belongs_to_collection"] for row in franchise_summary.collect()]
    assert franchise_collections == ["Franchise A"]
    assert None not in franchise_collections

    assert [row["director"] for row in director_summary.collect()] == ["Director B", "Director C"]
    assert director_summary.collect()[0]["total_revenue"] == pytest.approx(150.0)


def test_franchise_success_excludes_standalone_and_blank_collections(spark):
    """Only real collections are franchises - null, empty, and whitespace are not."""
    df = spark.createDataFrame(
        [
            Row(id=299534, title="Collection Movie 1", belongs_to_collection="The Avengers Collection", revenue_musd=100.0, budget_musd=50.0, vote_average=8.0),
            Row(id=299536, title="Collection Movie 2", belongs_to_collection="The Avengers Collection", revenue_musd=200.0, budget_musd=60.0, vote_average=7.0),
            Row(id=597, title="Null Collection", belongs_to_collection=None, revenue_musd=900.0, budget_musd=200.0, vote_average=7.9),
            Row(id=19995, title="Empty Collection", belongs_to_collection="", revenue_musd=800.0, budget_musd=237.0, vote_average=7.6),
            Row(id=140607, title="Whitespace Collection", belongs_to_collection="   ", revenue_musd=700.0, budget_musd=245.0, vote_average=7.3),
        ]
    )

    summary = kpis.franchise_success(df).collect()
    collections = [row["belongs_to_collection"] for row in summary]

    # The standalone rows carry the three highest revenues, so if they were still
    # grouped they would top this leaderboard rather than merely appear in it.
    assert collections == ["The Avengers Collection"]
    assert summary[0]["num_movies"] == 2
    assert summary[0]["total_revenue"] == pytest.approx(300.0)


def test_director_success_split_co_directed_movies_into_individual_credit(spark):
    df = spark.createDataFrame(
        [
            Row(id=1, title="Co-Directed Film", director="Anthony Russo|Joe Russo", revenue_musd=120.0, vote_average=8.5),
            Row(id=2, title="Solo Film", director="Christopher Nolan", revenue_musd=80.0, vote_average=7.5),
            Row(id=3, title="Missing Director", director=None, revenue_musd=30.0, vote_average=6.0),
            Row(id=4, title="Empty Director", director="", revenue_musd=10.0, vote_average=5.5),
            Row(id=5, title="Whitespace Director", director="   Anthony Russo   |   Joe Russo   ", revenue_musd=50.0, vote_average=7.0),
        ]
    )

    director_summary = kpis.director_success(df)
    rows = {row["director"]: row for row in director_summary.collect()}

    assert rows["Anthony Russo"]["num_movies"] == 2
    assert rows["Anthony Russo"]["total_revenue"] == pytest.approx(170.0)
    assert rows["Joe Russo"]["num_movies"] == 2
    assert rows["Joe Russo"]["total_revenue"] == pytest.approx(170.0)
    assert rows["Christopher Nolan"]["num_movies"] == 1
    assert rows["Christopher Nolan"]["total_revenue"] == pytest.approx(80.0)
    assert set(rows) == {"Anthony Russo", "Joe Russo", "Christopher Nolan"}


def test_visualization_chart_functions_create_output_files(tmp_path, monkeypatch):
    output_dir = tmp_path / "outputs"
    monkeypatch.setattr(visualize, "OUTPUT_DIR", output_dir)

    df = pd.DataFrame(
        [
            {
                "title": "Movie One",
                "budget_musd": 100.0,
                "revenue_musd": 250.0,
                "genres": "Action|Sci-Fi",
                "roi": 1.5,
                "vote_average": 8.5,
                "popularity": 70.0,
                "release_date": "2020-01-15",
            },
            {
                "title": "Movie Two",
                "budget_musd": 50.0,
                "revenue_musd": 90.0,
                "genres": "Drama|Action",
                "roi": 0.8,
                "vote_average": 7.2,
                "popularity": 45.0,
                "release_date": "2021-04-20",
            },
        ]
    )

    comparison = pd.DataFrame(
        {
            "mean_revenue": [200.0, 150.0],
            "median_roi": [1.6, 1.1],
            "mean_budget": [75.0, 60.0],
            "mean_popularity": [60.0, 40.0],
            "mean_rating": [8.0, 7.0],
        },
        index=pd.Index(["Franchise", "Standalone"], name="franchise_status"),
    )

    paths = [
        visualize.plot_revenue_vs_budget(df, show=False),
        visualize.plot_roi_by_genre(df, show=False),
        visualize.plot_popularity_vs_rating(df, show=False),
        visualize.plot_yearly_trends(df, show=False),
        visualize.plot_franchise_vs_standalone(comparison, show=False),
    ]

    assert len(paths) == 5
    assert all(path.exists() for path in paths)
    assert all(path.suffix == ".png" for path in paths)


@pytest.mark.skipif(
    not RAW_DATA_PATH.exists(),
    reason=(
        f"Frozen raw input not present at {RAW_DATA_PATH}. "
        "data/raw/ is gitignored, so this baseline runs only where the locked "
        "TMDb pull has been fetched (see README ingestion step)."
    ),
)
def test_frozen_spark_baseline_outputs_regression(spark):
    """
    Regression-check the current Spark pipeline against a frozen Spark baseline.

    Classification: this is a Spark frozen-output regression test, NOT a
    Pandas-to-Spark parity test. Every expected value in
    tests/fixtures/frozen_dataset_spark_outputs.json was captured from an
    earlier run of THIS Spark pipeline over the locked 18-movie raw JSON pull.
    The fixture therefore proves the results have not drifted; it does not
    independently prove they are correct. Independent cross-engine validation
    belongs to the separate Pandas-vs-Spark parity test still to be added.

    Only deterministic cleaning and KPI outputs are compared - no live TMDb API
    calls, and no vote-derived fields (popularity, vote_count, and any mean_rating
    aggregate of vote_average), all of which move as real users keep voting.
    """
    fixture_path = FIXTURES_DIR / "frozen_dataset_spark_outputs.json"
    expected = json.loads(fixture_path.read_text(encoding="utf-8"))

    df = clean_dataframe(load_raw_json(spark))
    df = kpis.add_financial_columns(df)
    df = kpis.add_franchise_status(df)

    # profit_musd and roi are read back from add_financial_columns rather than
    # recomputed here, so the frozen values guard the pipeline's own arithmetic.
    top_revenue = (
        df.orderBy(df.revenue_musd.desc())
        .select("title", "revenue_musd", "budget_musd")
        .limit(5)
        .toPandas()
        .to_dict("records")
    )
    top_profit = (
        df.orderBy("profit_musd", ascending=False)
        .select("title", "profit_musd", "budget_musd", "revenue_musd")
        .limit(5)
        .toPandas()
        .to_dict("records")
    )
    top_roi = (
        df.filter(df.budget_musd >= 10)
        .orderBy("roi", ascending=False)
        .select("title", "roi", "budget_musd", "revenue_musd")
        .limit(5)
        .toPandas()
        .to_dict("records")
    )
    # mean_rating is dropped from both aggregate baselines. It averages
    # vote_average, which real TMDb users keep moving, so freezing it made this
    # test fail on any refreshed raw pull - drift in the source data, not a
    # regression in the pipeline. Revenue, budget and counts do not move that way.
    franchise_summary = (
        kpis.franchise_success(df)
        .orderBy("total_revenue", ascending=False)
        .drop("mean_rating")
        .limit(3)
        .toPandas()
        .to_dict("records")
    )
    director_summary = (
        kpis.director_success(df)
        .orderBy("total_revenue", ascending=False)
        .drop("mean_rating")
        .limit(3)
        .toPandas()
        .to_dict("records")
    )

    assert top_revenue == expected["top_revenue"], "Top revenue mismatch"
    assert top_profit == expected["top_profit"], "Top profit mismatch"
    assert top_roi == expected["top_roi_10m"], "Top ROI (10M budget filter) mismatch"

    # franchise_top_3 was re-frozen after franchise_success() stopped grouping
    # standalone movies into one unnamed null "collection". The null group that
    # used to rank 2nd is gone, so these three rows are all real collections.
    assert franchise_summary == expected["franchise_top_3"], "Top 3 franchises mismatch"
    assert all(row["belongs_to_collection"] for row in franchise_summary), "Null/blank collection leaked into franchise results"

    # Anthony Russo and Joe Russo tie exactly on total_revenue, so compare by
    # director name instead of depending on how Spark breaks that tie.
    assert sorted(director_summary, key=lambda row: row["director"]) == sorted(expected["director_top_3"], key=lambda row: row["director"]), "Top directors mismatch"
