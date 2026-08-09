"""
visualize.py

Chart-generation functions for the movies dataset. Every function
saves its chart to outputs/ as a PNG file, in addition to optionally
displaying it, so visual results are captured as real artifacts
rather than only ever appearing transiently via plt.show().

Note: these functions expect pandas DataFrames, not Spark DataFrames.
Matplotlib has no native support for Spark, and chart inputs here are
always small, already-aggregated results (e.g. yearly totals, a
handful of comparison rows) — never the full raw dataset. Any Spark
DataFrame must be converted with .toPandas() at the call site (in
movie_pipeline.py), right before being passed into these functions,
so the conversion point stays explicit and deliberate rather than
hidden inside plotting code.
"""

import sys
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

sys.path.append(str(Path(__file__).resolve().parent.parent))
from monitoring.logging_config import get_logger

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = PROJECT_ROOT / "outputs"


def _save_and_show(filename: str, show: bool = True) -> Path:
    """
    Shared helper: save the current Matplotlib figure to outputs/,
    creating the directory if needed, then optionally display it.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIR / filename
    plt.savefig(path, dpi=150)
    logger.info("Saved chart to %s", path)

    if show:
        plt.show()
    else:
        plt.close()

    return path


def plot_revenue_vs_budget(df: pd.DataFrame, show: bool = True) -> Path:
    """Scatter plot of revenue against budget, labeled with movie titles."""
    plt.figure(figsize=(6, 4))
    plt.scatter(df["budget_musd"], df["revenue_musd"], color="#1f77b4")

    for _, row in df.iterrows():
        plt.annotate(row["title"], (row["budget_musd"], row["revenue_musd"]), fontsize=6, alpha=0.7)

    plt.xlabel("Budget (million USD)")
    plt.ylabel("Revenue (million USD)")
    plt.title("Revenue vs. Budget")
    plt.grid(False)
    plt.tight_layout()
    return _save_and_show("revenue_vs_budget.png", show=show)


def plot_roi_by_genre(df: pd.DataFrame, show: bool = True) -> Path:
    """Boxplot of ROI distribution per genre (movies exploded across all their genres)."""
    df_genres = df.copy()
    df_genres["genre_list"] = df_genres["genres"].str.split("|")
    df_genres = df_genres.explode("genre_list")

    grouped = df_genres.groupby("genre_list")["roi"]
    labels = list(grouped.groups.keys())
    data = [grouped.get_group(g).dropna().values for g in labels]

    plt.figure(figsize=(8, 4))
    plt.boxplot(data, tick_labels=labels)
    plt.xlabel("Genre")
    plt.ylabel("ROI")
    plt.title("ROI Distribution by Genre")
    plt.xticks(rotation=45)
    plt.grid(False)
    plt.tight_layout()
    return _save_and_show("roi_by_genre.png", show=show)


def plot_popularity_vs_rating(df: pd.DataFrame, show: bool = True) -> Path:
    """Scatter plot of popularity against rating, labeled with movie titles."""
    plt.figure(figsize=(6, 4))
    plt.scatter(df["vote_average"], df["popularity"], color="#1f77b4")

    for _, row in df.iterrows():
        plt.annotate(row["title"], (row["vote_average"], row["popularity"]), fontsize=6, alpha=0.7)

    plt.xlabel("Rating (vote average)")
    plt.ylabel("Popularity")
    plt.title("Popularity vs. Rating")
    plt.grid(False)
    plt.tight_layout()
    return _save_and_show("popularity_vs_rating.png", show=show)


def plot_yearly_trends(df: pd.DataFrame, show: bool = True) -> Path:
    """Line chart of total revenue per release year."""
    df = df.copy()
    df["year"] = df["release_date"].dt.year
    yearly_revenue = df.groupby("year")["revenue_musd"].sum()

    plt.figure(figsize=(8, 4))
    plt.plot(yearly_revenue.index, yearly_revenue.values, marker="o", color="#1f77b4")
    plt.xlabel("Year")
    plt.ylabel("Total Revenue (million USD)")
    plt.title("Yearly Trends in Box Office Performance")
    plt.grid(False)
    plt.tight_layout()
    return _save_and_show("yearly_trends.png", show=show)


def plot_franchise_vs_standalone(comparison: pd.DataFrame, show: bool = True) -> Path:
    """
    Grid of small bar charts, one per metric, comparing Franchise vs.
    Standalone movies. Takes the already-computed comparison summary
    (see kpis.compare_franchise_vs_standalone) rather than the raw
    DataFrame, since the aggregation belongs in kpis.py, not here.
    """
    fig, axes = plt.subplots(1, len(comparison.columns), figsize=(15, 3))

    for ax, metric in zip(axes, comparison.columns):
        ax.bar(comparison.index, comparison[metric], color="#1f77b4")
        ax.set_title(metric)
        ax.grid(False)

    plt.tight_layout()
    return _save_and_show("franchise_vs_standalone.png", show=show)


def generate_all_charts(df: pd.DataFrame, comparison: pd.DataFrame, show: bool = True) -> None:
    """Generate and save all five required charts in one call."""
    plot_revenue_vs_budget(df, show=show)
    plot_roi_by_genre(df, show=show)
    plot_popularity_vs_rating(df, show=show)
    plot_yearly_trends(df, show=show)
    plot_franchise_vs_standalone(comparison, show=show)
    logger.info("Generated and saved all charts to %s", OUTPUT_DIR)