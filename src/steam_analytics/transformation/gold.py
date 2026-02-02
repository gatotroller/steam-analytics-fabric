"""
Gold layer transformations - Business aggregations.

This module creates BI-ready aggregated tables from Silver layer data.
Gold tables are optimized for querying and reporting, not for history tracking.

Usage in notebooks:
    >>> from src.steam_analytics.transformation.gold import run_gold_transform
    >>> results = run_gold_transform(spark, silver_path, gold_path)
"""

from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
)

# Try to import DeltaTable
try:
    from delta.tables import DeltaTable

    DELTA_AVAILABLE = True
except ImportError:
    DELTA_AVAILABLE = False
    DeltaTable = None  # type: ignore


# =============================================================================
# Data Classes for Results
# =============================================================================


@dataclass
class GoldTableResult:
    """Result of a Gold table build operation."""

    table_name: str
    records_written: int
    success: bool
    error_message: str | None = None


@dataclass
class GoldTransformResult:
    """Result of complete Gold transformation."""

    started_at: datetime
    completed_at: datetime
    results: dict[str, GoldTableResult]

    @property
    def total_records(self) -> int:
        return sum(r.records_written for r in self.results.values())

    @property
    def all_successful(self) -> bool:
        return all(r.success for r in self.results.values())

    @property
    def duration_seconds(self) -> float:
        return (self.completed_at - self.started_at).total_seconds()


# =============================================================================
# Silver Data Loading Helpers
# =============================================================================


def load_current_games(spark: SparkSession, silver_path: str) -> DataFrame:
    """Load current (is_current=true) records from dim_games."""
    return (
        spark.read.format("delta")
        .load(f"{silver_path}/Tables/dim_games")
        .filter(F.col("is_current"))
    )


def load_all_games_history(spark: SparkSession, silver_path: str) -> DataFrame:
    """Load all records (including historical) from dim_games."""
    return spark.read.format("delta").load(f"{silver_path}/Tables/dim_games")


def load_current_reviews(spark: SparkSession, silver_path: str) -> DataFrame:
    """Load current records from dim_game_reviews."""
    return (
        spark.read.format("delta")
        .load(f"{silver_path}/Tables/dim_game_reviews")
        .filter(F.col("is_current"))
    )


def load_latest_player_counts(spark: SparkSession, silver_path: str) -> DataFrame:
    """
    Load the most recent player count per game.
    OPTIMIZED: Uses partition pruning instead of scanning full history.
    """
    table_path = f"{silver_path}/Tables/fact_player_counts"

    # 1. Get the latest available snapshot date efficiently
    try:
        max_date_row = (
            spark.read.format("delta").load(table_path).agg(F.max("snapshot_date")).collect()
        )
        max_date = max_date_row[0][0]
    except Exception:
        # Fallback if table is empty
        return spark.read.format("delta").load(table_path)

    if not max_date:
        return spark.read.format("delta").load(table_path)

    print(f"   Filtering player counts for latest snapshot: {max_date}")

    # 2. Read ONLY that partition (Push down filter)
    df = spark.read.format("delta").load(table_path).filter(F.col("snapshot_date") == max_date)

    # 3. Deduplicate (Just in case there are multiple runs for the same date)
    return df.dropDuplicates(["app_id"])


# =============================================================================
# Gold Table Builders
# =============================================================================


def build_game_metrics(
    spark: SparkSession,
    silver_path: str,
    gold_path: str,
) -> GoldTableResult:
    """
    Build agg_game_metrics - Current state of each game.
    """
    print("Building agg_game_metrics...")

    try:
        # Load Silver data
        print("   Loading Silver tables...")
        games_df = load_current_games(spark, silver_path)
        reviews_df = load_current_reviews(spark, silver_path)
        players_df = load_latest_player_counts(spark, silver_path)

        print(
            f"   Games: {games_df.count()}, Reviews: {reviews_df.count()}, Players: {players_df.count()}"
        )

        # Join all sources
        print("   Joining data...")
        metrics_df = (
            games_df.alias("g")
            .join(
                reviews_df.alias("r"),
                F.col("g.app_id") == F.col("r.app_id"),
                "left",
            )
            .join(
                players_df.alias("p"),
                F.col("g.app_id") == F.col("p.app_id"),
                "left",
            )
            .select(
                # Game identifiers
                F.col("g.app_id"),
                F.col("g.name"),
                F.col("g.type"),
                F.col("g.is_free"),
                # Price info
                F.col("g.price_currency"),
                F.col("g.price_initial_cents"),
                F.col("g.price_final_cents"),
                F.col("g.price_discount_percent"),
                # Computed price fields
                (F.col("g.price_final_cents") / 100).cast(DoubleType()).alias("price_usd"),
                F.when(F.col("g.price_discount_percent") > 0, True)
                .otherwise(False)
                .alias("is_on_sale"),
                # Categories
                F.col("g.genres"),
                F.col("g.categories"),
                F.col("g.developers"),
                F.col("g.publishers"),
                # Platforms
                F.col("g.platforms_windows"),
                F.col("g.platforms_mac"),
                F.col("g.platforms_linux"),
                # Metacritic
                F.col("g.metacritic_score"),
                # Review metrics
                F.col("r.total_reviews"),
                F.col("r.total_positive"),
                F.col("r.total_negative"),
                F.col("r.positive_ratio"),
                F.col("r.review_score"),
                F.col("r.review_score_desc"),
                # Player metrics
                F.col("p.player_count").alias("current_players"),
                F.col("p.snapshot_timestamp").alias("player_count_updated_at"),
                # Release info
                F.col("g.release_date"),
                F.col("g.coming_soon"),
                # Audit
                F.current_timestamp().alias("_refreshed_at"),
            )
        )

        # Calculate popularity score (custom metric)
        metrics_df = metrics_df.withColumn(
            "popularity_score",
            F.round(
                F.log1p(F.coalesce(F.col("current_players"), F.lit(0)))
                * F.coalesce(F.col("positive_ratio"), F.lit(0.5))
                * 100,
                2,
            ),
        )

        # Write to Gold
        target_path = f"{gold_path}/Tables/agg_game_metrics"
        record_count = metrics_df.count()

        print(f"   Writing {record_count} records...")

        metrics_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
            target_path
        )

        print(f"   SUCCESS: agg_game_metrics ({record_count} records)")

        return GoldTableResult(
            table_name="agg_game_metrics",
            records_written=record_count,
            success=True,
        )

    except Exception as e:
        print(f"   FAILED: {e}")
        return GoldTableResult(
            table_name="agg_game_metrics",
            records_written=0,
            success=False,
            error_message=str(e),
        )


def build_price_history(
    spark: SparkSession,
    silver_path: str,
    gold_path: str,
) -> GoldTableResult:
    """
    Build agg_price_history - Historical price changes from SCD2.
    """
    print("Building agg_price_history...")

    try:
        # Load ALL games history (SCD2 records)
        print("   Loading dim_games history...")
        games_history = load_all_games_history(spark, silver_path)

        total_records = games_history.count()
        print(f"   Found {total_records} historical records")

        # Transform to price history format
        print("   Transforming...")
        price_history_df = games_history.select(
            # Keys
            F.col("app_id"),
            F.col("name"),
            # SCD2 validity period
            F.col("valid_from").alias("price_effective_from"),
            F.col("valid_to").alias("price_effective_to"),
            F.col("is_current"),
            # Price data
            F.col("price_currency"),
            F.col("price_initial_cents"),
            F.col("price_final_cents"),
            F.col("price_discount_percent"),
            # Computed fields
            (F.col("price_initial_cents") / 100).cast(DoubleType()).alias("price_initial_usd"),
            (F.col("price_final_cents") / 100).cast(DoubleType()).alias("price_final_usd"),
            ((F.col("price_initial_cents") - F.col("price_final_cents")) / 100)
            .cast(DoubleType())
            .alias("discount_amount_usd"),
            F.when(F.col("price_discount_percent") > 0, True)
            .otherwise(False)
            .alias("is_discounted"),
            # Categorize discount
            F.when(F.col("price_discount_percent") == 0, "No Discount")
            .when(F.col("price_discount_percent") < 25, "Small (1-24%)")
            .when(F.col("price_discount_percent") < 50, "Medium (25-49%)")
            .when(F.col("price_discount_percent") < 75, "Large (50-74%)")
            .otherwise("Huge (75%+)")
            .alias("discount_tier"),
            # Genres for filtering
            F.col("genres"),
            # Audit
            F.current_timestamp().alias("_refreshed_at"),
            # Partition column (extract date from valid_from)
            F.date_format(F.col("valid_from"), "yyyy-MM-dd").alias("effective_date"),
        )

        # Write to Gold
        target_path = f"{gold_path}/Tables/agg_price_history"
        record_count = price_history_df.count()

        print(f"   Writing {record_count} records...")

        # OPTIMIZED: Removed partitionBy("is_current") to avoid skewed partitions
        price_history_df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(target_path)

        print(f"   SUCCESS: agg_price_history ({record_count} records)")

        return GoldTableResult(
            table_name="agg_price_history",
            records_written=record_count,
            success=True,
        )

    except Exception as e:
        print(f"   FAILED: {e}")
        return GoldTableResult(
            table_name="agg_price_history",
            records_written=0,
            success=False,
            error_message=str(e),
        )


def build_genre_summary(
    spark: SparkSession,
    silver_path: str,
    gold_path: str,
) -> GoldTableResult:
    """
    Build agg_genre_summary - Market metrics aggregated by genre.
    """
    print("Building agg_genre_summary...")

    try:
        # Load current games and reviews
        print("   Loading Silver data...")
        games_df = load_current_games(spark, silver_path)
        reviews_df = load_current_reviews(spark, silver_path)
        players_df = load_latest_player_counts(spark, silver_path)

        # Join games with reviews and players
        print("   Joining data...")
        combined_df = (
            games_df.alias("g")
            .join(
                reviews_df.alias("r"),
                F.col("g.app_id") == F.col("r.app_id"),
                "left",
            )
            .join(
                players_df.alias("p"),
                F.col("g.app_id") == F.col("p.app_id"),
                "left",
            )
            .select(
                F.col("g.app_id"),
                F.col("g.name"),
                F.col("g.genres"),
                F.col("g.is_free"),
                F.col("g.price_final_cents"),
                F.col("g.price_discount_percent"),
                F.col("g.metacritic_score"),
                F.col("r.total_reviews"),
                F.col("r.positive_ratio"),
                F.col("r.review_score"),
                F.col("p.player_count"),
            )
        )

        # Explode genres array - each game appears once per genre
        print("   Exploding genres...")
        exploded_df = combined_df.withColumn("genre", F.explode_outer(F.col("genres")))

        # Handle null genres
        exploded_df = exploded_df.withColumn("genre", F.coalesce(F.col("genre"), F.lit("Unknown")))

        # Aggregate by genre
        print("   Aggregating by genre...")
        genre_summary_df = (
            exploded_df.groupBy("genre")
            .agg(
                # Counts
                F.countDistinct("app_id").alias("game_count"),
                F.sum(F.when(F.col("is_free"), 1).otherwise(0)).alias("free_games_count"),
                # Price metrics (exclude free games)
                F.round(
                    F.avg(F.when(~F.col("is_free"), F.col("price_final_cents") / 100)),
                    2,
                ).alias("avg_price_usd"),
                F.round(
                    F.min(F.when(~F.col("is_free"), F.col("price_final_cents") / 100)),
                    2,
                ).alias("min_price_usd"),
                F.round(
                    F.max(F.when(~F.col("is_free"), F.col("price_final_cents") / 100)),
                    2,
                ).alias("max_price_usd"),
                # Discount metrics
                F.round(F.avg("price_discount_percent"), 2).alias("avg_discount_percent"),
                F.sum(F.when(F.col("price_discount_percent") > 0, 1).otherwise(0)).alias(
                    "games_on_sale_count"
                ),
                # Review metrics
                F.sum("total_reviews").alias("total_reviews"),
                F.round(F.avg("positive_ratio"), 4).alias("avg_positive_ratio"),
                F.round(F.avg("review_score"), 2).alias("avg_review_score"),
                # Metacritic
                F.round(F.avg("metacritic_score"), 2).alias("avg_metacritic_score"),
                F.count(F.when(F.col("metacritic_score").isNotNull(), 1)).alias(
                    "games_with_metacritic"
                ),
                # Player metrics
                F.sum("player_count").alias("total_current_players"),
                F.round(F.avg("player_count"), 0).alias("avg_players_per_game"),
                F.max("player_count").alias("max_players_single_game"),
            )
            .withColumn(
                "percent_free",
                F.round(F.col("free_games_count") / F.col("game_count") * 100, 2),
            )
            .withColumn(
                "percent_on_sale",
                F.round(F.col("games_on_sale_count") / F.col("game_count") * 100, 2),
            )
            .withColumn("_refreshed_at", F.current_timestamp())
            .orderBy(F.col("game_count").desc())
        )

        # Write to Gold
        target_path = f"{gold_path}/Tables/agg_genre_summary"
        record_count = genre_summary_df.count()

        print(f"   Writing {record_count} genres...")

        genre_summary_df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(target_path)

        print(f"   SUCCESS: agg_genre_summary ({record_count} records)")

        return GoldTableResult(
            table_name="agg_genre_summary",
            records_written=record_count,
            success=True,
        )

    except Exception as e:
        print(f"   FAILED: {e}")
        return GoldTableResult(
            table_name="agg_genre_summary",
            records_written=0,
            success=False,
            error_message=str(e),
        )


# =============================================================================
# Main Entry Point
# =============================================================================


def run_gold_transform(
    spark: SparkSession,
    silver_path: str,
    gold_path: str,
    *,
    include_game_metrics: bool = True,
    include_price_history: bool = True,
    include_genre_summary: bool = True,
) -> GoldTransformResult:
    """
    Run complete Gold layer transformation.
    """
    started_at = datetime.now()
    results: dict[str, GoldTableResult] = {}

    print("=" * 60)
    print("GOLD LAYER TRANSFORMATION")
    print("=" * 60)
    print(f"Silver: {silver_path}")
    print(f"Gold:   {gold_path}")
    print("=" * 60)

    if include_game_metrics:
        print("\n" + "-" * 40)
        print("TABLE: agg_game_metrics")
        print("-" * 40)
        results["agg_game_metrics"] = build_game_metrics(spark, silver_path, gold_path)

    if include_price_history:
        print("\n" + "-" * 40)
        print("TABLE: agg_price_history")
        print("-" * 40)
        results["agg_price_history"] = build_price_history(spark, silver_path, gold_path)

    if include_genre_summary:
        print("\n" + "-" * 40)
        print("TABLE: agg_genre_summary")
        print("-" * 40)
        results["agg_genre_summary"] = build_genre_summary(spark, silver_path, gold_path)

    completed_at = datetime.now()

    # Print summary
    print("\n" + "=" * 60)
    print("TRANSFORMATION SUMMARY")
    print("=" * 60)

    for table_name, result in results.items():
        status = "SUCCESS" if result.success else "FAILED"
        print(f"[{status}] {table_name}: {result.records_written} records")
        if result.error_message:
            print(f"          Error: {result.error_message}")

    duration = (completed_at - started_at).total_seconds()
    total = sum(r.records_written for r in results.values())

    print(f"\nTotal records written: {total}")
    print(f"Duration: {duration:.2f} seconds")
    print("=" * 60)

    return GoldTransformResult(
        started_at=started_at,
        completed_at=completed_at,
        results=results,
    )
