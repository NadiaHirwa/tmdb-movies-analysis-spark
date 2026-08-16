"""
api.py

Fetches movie data (including cast/crew credits) from the TMDb API
for a predefined list of movie IDs, and saves the raw results to disk.
"""

import os
import sys
import json
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

# Make src/ importable so we can reach monitoring/logging_config.py
sys.path.append(str(Path(__file__).resolve().parent.parent))
from monitoring.logging_config import get_logger

logger = get_logger(__name__)

load_dotenv()

REQUIRED_FIELDS = {"id", "title", "release_date", "genres", "credits", "budget", "revenue"}

API_KEY = os.getenv("TMDB_API_KEY")
BASE_URL = "https://api.themoviedb.org/3/movie"

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw" / "movies_raw.json"

sys.path.append(str(Path(__file__).resolve().parent.parent))
from monitoring.logging_config import get_logger
from utils.constants import MOVIE_IDS


def _validate_movie_contract(movie: dict | None) -> bool:
    """Reject malformed TMDb payloads before they reach the pipeline."""
    if not isinstance(movie, dict):
        return False

    missing_fields = [
        field for field in sorted(REQUIRED_FIELDS)
        if field not in movie or movie.get(field) in (None, "", [], {})
    ]

    if missing_fields:
        logger.warning(
            "Rejected malformed movie payload for id=%s. Missing required fields: %s",
            movie.get("id"),
            ", ".join(missing_fields),
        )
        return False

    return True


def fetch_movie(
    movie_id: int,
    retries: int = 3,
    backoff: float = 1.0,
    timeout: int = 10,
    session: requests.Session | None = None,
) -> dict | None:
    """
    Fetch a single movie's details, with cast/crew credits appended,
    from the TMDb API.

    Handles three distinct failure modes:
    - Invalid/non-existent movie IDs (HTTP 404): not retried, since
      retrying can never make an invalid ID valid.
    - Transient server errors (5xx) or unexpected HTTP errors: retried
      with linear backoff.
    - Network failures (timeout, connection errors): retried with
      linear backoff.

    Parameters
    ----------
    movie_id : int
        The TMDb movie ID to fetch.
    retries : int
        Number of attempts before giving up on a transient failure.
    backoff : float
        Base delay in seconds between retries; scales with attempt number.
    timeout : int
        Seconds to wait for a response before treating the request as failed.
    session : requests.Session | None
        Optional shared session for connection reuse across many requests.

    Returns
    -------
    dict | None
        The movie's JSON data, or None if the request could not be
        completed successfully after all retries.
    """
    url = f"{BASE_URL}/{movie_id}"
    params = {"api_key": API_KEY, "append_to_response": "credits"}
    http = session or requests

    for attempt in range(1, retries + 1):
        try:
            response = http.get(url, params=params, timeout=timeout)

            if response.status_code == 404:
                logger.warning("movie_id=%s not found (404 from API). Skipping.", movie_id)
                return None

            response.raise_for_status()
            movie = response.json()

            if not _validate_movie_contract(movie):
                logger.warning("movie_id=%s rejected because it failed the API contract check.", movie_id)
                return None

            logger.info("Fetched movie_id=%s (attempt %d).", movie_id, attempt)
            return movie

        except requests.exceptions.Timeout:
            logger.warning("Timeout fetching movie_id=%s on attempt %d.", movie_id, attempt)

        except requests.exceptions.HTTPError as exc:
            logger.warning("HTTP error for movie_id=%s on attempt %d: %s", movie_id, attempt, exc)

        except requests.exceptions.RequestException as exc:
            logger.warning("Network error for movie_id=%s on attempt %d: %s", movie_id, attempt, exc)

        if attempt < retries:
            time.sleep(backoff * attempt)

    logger.error("Giving up on movie_id=%s after %d attempts.", movie_id, retries)
    return None


def fetch_all_movies(movie_ids: list[int]) -> list[dict]:
    """
    Fetch data for a list of movie IDs, skipping any that fail.

    If every requested ID is rejected or fails, raise a clear error so the
    pipeline stops before saving an empty raw JSON file.
    """
    # Reusing a single Session is a lightweight optimization: it keeps TCP
    # connections alive so the client can reuse sockets across requests.
    # That matters much more once the number of API calls scales beyond this
    # small assignment, where the per-request overhead becomes noticeable.
    with requests.Session() as session:
        movies = []
        invalid_ids = []

        for movie_id in movie_ids:
            data = fetch_movie(movie_id, session=session)
            if data is not None:
                movies.append(data)
            else:
                invalid_ids.append(movie_id)

        if not movies:
            raise ValueError(
                "Zero usable movie records were fetched from TMDb. "
                f"Requested {len(movie_ids)} IDs, all were invalid or unusable."
            )

        if invalid_ids:
            logger.warning(
                "Some movie IDs were invalid or rejected and were skipped: %s",
                invalid_ids,
            )

        logger.info("Collected %d/%d movies successfully.", len(movies), len(movie_ids))
        return movies


def save_raw_data(movies: list[dict], path: Path = RAW_DATA_PATH) -> None:
    """
    Save fetched movie records to disk as JSON.

    Creates the destination directory first if it doesn't already
    exist, so this works correctly on a fresh clone of the repo where
    data/raw/ has never been created.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(movies, f, indent=2)
    logger.info("Saved %d records to %s", len(movies), path)


def run() -> None:
    """Run the full ingestion step: fetch all movies and save raw data."""
    if not API_KEY:
        logger.error("TMDB_API_KEY not set. Check your .env file.")
        return

    movies = fetch_all_movies(MOVIE_IDS)
    save_raw_data(movies)


if __name__ == "__main__":
    run()