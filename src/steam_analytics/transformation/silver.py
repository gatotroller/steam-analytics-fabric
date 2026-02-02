"""
Silver layer transformations with SCD Type 2.

This module contains all logic for transforming Bronze data to Silver,
including SCD Type 2 implementation for tracking historical changes.
Optimized for performance with distributed ID generation.

Usage in notebooks:
    >>> from src.steam_analytics.transformation.silver import run_silver_transform
    >>> results = run_silver_transform(spark, bronze_path, silver_path)
"""

from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

# Try to import DeltaTable, handle case where delta is not available
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
class TransformResult:
    """Result of a transformation operation."""

    table_name: str
    records_processed: int
    records_inserted: int
    records_updated: int
    records_unchanged: int
    success: bool
    error_message: str | None = None


@dataclass
class SilverTransformResult:
    """Result of complete Silver transformation."""

    started_at: datetime
    completed_at: datetime
    results: dict[str, TransformResult]

    @property
    def total_processed(self) -> int:
        return sum(r.records_processed for r in self.results.values())

    @property
    def total_inserted(self) -> int:
        return sum(r.records_inserted for r in self.results.values())

    @property
    def total_updated(self) -> int:
        return sum(r.records_updated for r in self.results.values())

    @property
    def all_successful(self) -> bool:
        return all(r.success for r in self.results.values())


# =============================================================================
# Bronze Data Loading
# =============================================================================


def load_bronze_json(
    spark: SparkSession,
    bronze_path: str,
    source_folder: str,
) -> DataFrame:
    """
    Load Bronze JSON files and extract records.

    Bronze files have structure:
    {
        "batch_id": "...",
        "source": "...",
        "records": [
            {"metadata": {...}, "raw_data": {...}}
        ]
    }

    Args:
        spark: SparkSession
        bronze_path: Base path to Bronze lakehouse (abfss://...)
        source_folder: Folder name (e.g., "raw_steam_store")

    Returns:
        DataFrame with flattened records
    """
    json_path = f"{bronze_path}/Files/bronze/{source_folder}/*/*.json"

    # Read JSON files
    df = spark.read.option("multiLine", "true").json(json_path)

    # Explode records array to get individual records
    # CORRECCIÓN: No seleccionamos 'batch_id' ni 'source' de la raíz aquí
    # para evitar duplicados, ya que vendrán dentro de metadata.*
    df = df.select(
        F.explode("records").alias("record"),
    )

    # Flatten metadata and raw_data
    # record.metadata.* ya trae 'batch_id', 'source', 'ingested_at', etc.
    df = df.select(
        F.col("record.metadata.*"),
        F.col("record.raw_data").alias("raw_data"),
    )

    return df


def load_bronze_store(spark: SparkSession, bronze_path: str) -> DataFrame:
    """
    Load and prepare Bronze store data for Silver transformation.
    """
    df = load_bronze_json(spark, bronze_path, "raw_steam_store")

    # Filter only successful records
    df = df.filter(F.col("status") == "success")

    # Extract fields from raw_data
    df = df.select(
        # Business key
        F.col("raw_data.steam_appid").cast(IntegerType()).alias("app_id"),
        # Attributes
        F.col("raw_data.name").cast(StringType()).alias("name"),
        F.col("raw_data.type").cast(StringType()).alias("type"),
        F.col("raw_data.is_free").cast(BooleanType()).alias("is_free"),
        F.col("raw_data.short_description").cast(StringType()).alias("short_description"),
        F.col("raw_data.developers").cast(ArrayType(StringType())).alias("developers"),
        F.col("raw_data.publishers").cast(ArrayType(StringType())).alias("publishers"),
        # Price (nested in price_overview)
        F.col("raw_data.price_overview.currency").cast(StringType()).alias("price_currency"),
        F.col("raw_data.price_overview.initial").cast(IntegerType()).alias("price_initial_cents"),
        F.col("raw_data.price_overview.final").cast(IntegerType()).alias("price_final_cents"),
        F.col("raw_data.price_overview.discount_percent")
        .cast(IntegerType())
        .alias("price_discount_percent"),
        # Platforms
        F.col("raw_data.platforms.windows").cast(BooleanType()).alias("platforms_windows"),
        F.col("raw_data.platforms.mac").cast(BooleanType()).alias("platforms_mac"),
        F.col("raw_data.platforms.linux").cast(BooleanType()).alias("platforms_linux"),
        # Metacritic
        F.col("raw_data.metacritic.score").cast(IntegerType()).alias("metacritic_score"),
        # Categories and Genres (extract descriptions)
        F.expr("transform(raw_data.categories, x -> x.description)").alias("categories"),
        F.expr("transform(raw_data.genres, x -> x.description)").alias("genres"),
        # Release date
        F.col("raw_data.release_date.date").cast(StringType()).alias("release_date"),
        F.col("raw_data.release_date.coming_soon").cast(BooleanType()).alias("coming_soon"),
        # Metadata for audit
        F.col("batch_id").alias("_source_batch_id"),
        F.to_timestamp(F.col("ingested_at")).alias("_ingested_at"),
    )

    # Remove duplicates - keep latest per app_id
    window = Window.partitionBy("app_id").orderBy(F.col("_ingested_at").desc())
    df = df.withColumn("_row_num", F.row_number().over(window))
    df = df.filter(F.col("_row_num") == 1).drop("_row_num")

    return df


def load_bronze_reviews(spark: SparkSession, bronze_path: str) -> DataFrame:
    """Load and prepare Bronze reviews data."""
    df = load_bronze_json(spark, bronze_path, "raw_steam_reviews")

    df = df.filter(F.col("status") == "success")

    df = df.select(
        F.col("raw_data.query_summary.num_reviews").cast(IntegerType()).alias("app_id_temp"),
        F.col("request_params.app_id").cast(IntegerType()).alias("app_id"),
        F.col("raw_data.query_summary.total_reviews").cast(IntegerType()).alias("total_reviews"),
        F.col("raw_data.query_summary.total_positive").cast(IntegerType()).alias("total_positive"),
        F.col("raw_data.query_summary.total_negative").cast(IntegerType()).alias("total_negative"),
        F.col("raw_data.query_summary.review_score").cast(IntegerType()).alias("review_score"),
        F.col("raw_data.query_summary.review_score_desc")
        .cast(StringType())
        .alias("review_score_desc"),
        F.col("batch_id").alias("_source_batch_id"),
        F.to_timestamp(F.col("ingested_at")).alias("_ingested_at"),
    )

    # Calculate positive ratio
    df = df.withColumn(
        "positive_ratio",
        F.when(
            F.col("total_reviews") > 0,
            F.col("total_positive").cast(DoubleType()) / F.col("total_reviews"),
        ).otherwise(None),
    )

    # Dedupe
    window = Window.partitionBy("app_id").orderBy(F.col("_ingested_at").desc())
    df = df.withColumn("_row_num", F.row_number().over(window))
    df = df.filter(F.col("_row_num") == 1).drop("_row_num", "app_id_temp")

    return df


def load_bronze_players(spark: SparkSession, bronze_path: str) -> DataFrame:
    """Load and prepare Bronze player stats data."""
    df = load_bronze_json(spark, bronze_path, "raw_steam_player_stats")

    df = df.filter(F.col("status") == "success")

    df = df.select(
        F.col("request_params.app_id").cast(IntegerType()).alias("app_id"),
        F.to_timestamp(F.col("ingested_at")).alias("snapshot_timestamp"),
        F.col("raw_data.player_count").cast(IntegerType()).alias("player_count"),
        F.col("batch_id").alias("_source_batch_id"),
    )

    # Add snapshot_date for partitioning
    df = df.withColumn("snapshot_date", F.date_format(F.col("snapshot_timestamp"), "yyyy-MM-dd"))

    return df


# =============================================================================
# SCD Type 2 Implementation
# =============================================================================


def apply_scd2_merge(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    key_column: str,
    tracked_columns: list[str],
    surrogate_key_column: str,
) -> TransformResult:
    """
    Apply SCD Type 2 merge logic to a Delta table.
    OPTIMIZED: Uses distributed ID generation + Merge Schema enabled.
    """
    if not DELTA_AVAILABLE:
        return TransformResult(
            table_name=target_path.split("/")[-1],
            records_processed=0,
            records_inserted=0,
            records_updated=0,
            records_unchanged=0,
            success=False,
            error_message="Delta Lake not available",
        )

    # --- Helper Optimization Function ---
    def add_sequential_id(df: DataFrame, start_val: int, col_name: str) -> DataFrame:
        """Adds a sequential ID distributedly (avoids Window.orderBy bottleneck)."""
        rdd = df.rdd.zipWithIndex().map(lambda x: (x[1] + start_val + 1, *x[0]))
        # CORRECCIÓN RUF005: Usar unpacking (*) en lugar de concatenación (+)
        new_schema = StructType([StructField(col_name, LongType(), False), *df.schema.fields])
        return spark.createDataFrame(rdd, new_schema)

    # ------------------------------------

    now = F.current_timestamp()
    far_future = F.lit("9999-12-31 23:59:59").cast(TimestampType())

    source_count = source_df.count()

    # Check if target table exists and has data
    try:
        target_table = DeltaTable.forPath(spark, target_path)
        target_df = spark.read.format("delta").load(target_path)
        target_exists = target_df.count() > 0
    except Exception:
        target_exists = False

    if not target_exists:
        # First load - insert all as current records
        max_sk = 0

        base_df = (
            source_df.withColumn("valid_from", now)
            .withColumn("valid_to", far_future)
            .withColumn("is_current", F.lit(True))
            .withColumn("_created_at", now)
            .withColumn("_updated_at", now)
        )

        initial_df = add_sequential_id(base_df, max_sk, surrogate_key_column)

        initial_df.write.format("delta").option("mergeSchema", "true").mode("append").save(
            target_path
        )

        return TransformResult(
            table_name=target_path.split("/")[-1],
            records_processed=source_count,
            records_inserted=source_count,
            records_updated=0,
            records_unchanged=0,
            success=True,
        )

    # CORRECCIÓN E712: Eliminar '== True', usar la columna booleana directamente
    current_df = spark.read.format("delta").load(target_path).filter(F.col("is_current"))

    # Create hash of tracked columns for change detection
    def create_hash_column(df: DataFrame, columns: list[str], alias: str) -> DataFrame:
        hash_expr = F.concat_ws(
            "||", *[F.coalesce(F.col(c).cast(StringType()), F.lit("__NULL__")) for c in columns]
        )
        return df.withColumn(alias, F.md5(hash_expr))

    source_hashed = create_hash_column(source_df, tracked_columns, "_source_hash")
    current_hashed = create_hash_column(current_df, tracked_columns, "_target_hash")

    # Find changed records
    changed_df = (
        source_hashed.alias("s")
        .join(current_hashed.alias("t"), F.col(f"s.{key_column}") == F.col(f"t.{key_column}"))
        .filter(F.col("s._source_hash") != F.col("t._target_hash"))
        .select("s.*")
        .drop("_source_hash")
    )

    # Find new records
    new_df = (
        source_hashed.alias("s")
        .join(
            current_hashed.alias("t"),
            F.col(f"s.{key_column}") == F.col(f"t.{key_column}"),
            "left_anti",
        )
        .drop("_source_hash")
    )

    changed_count = changed_df.count()
    new_count = new_df.count()
    unchanged_count = source_count - changed_count - new_count

    # Get max surrogate key
    try:
        max_sk_row = current_df.agg(F.max(surrogate_key_column)).collect()[0]
        max_sk = max_sk_row[0] or 0
    except Exception:
        max_sk = 0

    # Process changes
    if changed_count > 0:
        changed_keys = [row[key_column] for row in changed_df.select(key_column).collect()]

        target_table = DeltaTable.forPath(spark, target_path)
        # CORRECCIÓN E712: Eliminar '== True' dentro de la condición del update
        target_table.update(
            condition=(F.col(key_column).isin(changed_keys)) & (F.col("is_current")),
            set={"is_current": F.lit(False), "valid_to": now, "_updated_at": now},
        )

        base_changed_df = (
            changed_df.withColumn("valid_from", now)
            .withColumn("valid_to", far_future)
            .withColumn("is_current", F.lit(True))
            .withColumn("_created_at", now)
            .withColumn("_updated_at", now)
        )

        changed_insert_df = add_sequential_id(base_changed_df, max_sk, surrogate_key_column)

        changed_insert_df.write.format("delta").option("mergeSchema", "true").mode("append").save(
            target_path
        )

        max_sk += changed_count

    # Process new records
    if new_count > 0:
        base_new_df = (
            new_df.withColumn("valid_from", now)
            .withColumn("valid_to", far_future)
            .withColumn("is_current", F.lit(True))
            .withColumn("_created_at", now)
            .withColumn("_updated_at", now)
        )

        new_insert_df = add_sequential_id(base_new_df, max_sk, surrogate_key_column)

        new_insert_df.write.format("delta").option("mergeSchema", "true").mode("append").save(
            target_path
        )

    return TransformResult(
        table_name=target_path.split("/")[-1],
        records_processed=source_count,
        records_inserted=new_count,
        records_updated=changed_count,
        records_unchanged=unchanged_count,
        success=True,
    )


# =============================================================================
# Table-Specific Transformations
# =============================================================================


def transform_games(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str,
) -> TransformResult:
    """
    Transform Bronze store data to Silver dim_games with SCD2.
    """
    print("Loading Bronze store data...")
    source_df = load_bronze_store(spark, bronze_path)
    print(f"   Loaded {source_df.count()} records")

    # Define columns to track for changes
    tracked_columns = [
        "price_final_cents",
        "price_discount_percent",
        "metacritic_score",
    ]

    print("Applying SCD Type 2 to dim_games...")
    target_path = f"{silver_path}/Tables/dim_games"

    result = apply_scd2_merge(
        spark=spark,
        source_df=source_df,
        target_path=target_path,
        key_column="app_id",
        tracked_columns=tracked_columns,
        surrogate_key_column="game_sk",
    )

    print(f"   Processed: {result.records_processed}")
    print(f"   Inserted: {result.records_inserted}")
    print(f"   Updated: {result.records_updated}")
    print(f"   Unchanged: {result.records_unchanged}")

    return result


def transform_reviews(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str,
) -> TransformResult:
    """
    Transform Bronze reviews data to Silver dim_game_reviews with SCD2.
    """
    print("Loading Bronze reviews data...")
    source_df = load_bronze_reviews(spark, bronze_path)
    print(f"   Loaded {source_df.count()} records")

    tracked_columns = [
        "total_reviews",
        "positive_ratio",
        "review_score",
    ]

    print("Applying SCD Type 2 to dim_game_reviews...")
    target_path = f"{silver_path}/Tables/dim_game_reviews"

    result = apply_scd2_merge(
        spark=spark,
        source_df=source_df,
        target_path=target_path,
        key_column="app_id",
        tracked_columns=tracked_columns,
        surrogate_key_column="review_sk",
    )

    print(f"   Processed: {result.records_processed}")
    print(f"   Inserted: {result.records_inserted}")
    print(f"   Updated: {result.records_updated}")
    print(f"   Unchanged: {result.records_unchanged}")

    return result


def transform_player_counts(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str,
) -> TransformResult:
    """
    Transform Bronze player stats to Silver fact_player_counts.
    OPTIMIZED: Prevents duplicates by filtering out already processed batches.
    """
    print("Loading Bronze player stats...")
    source_df = load_bronze_players(spark, bronze_path)
    total_source_count = source_df.count()
    print(f"   Found {total_source_count} records in Bronze")

    target_path = f"{silver_path}/Tables/fact_player_counts"

    # --- Deduplication Logic ---
    # Check if table exists to filter out already processed batches
    if DELTA_AVAILABLE and DeltaTable.isDeltaTable(spark, target_path):
        print("   Checking for existing batches in Silver...")
        existing_batches = (
            spark.read.format("delta").load(target_path).select("_source_batch_id").distinct()
        )

        # Filter source: Keep only records where batch_id is NOT in existing_batches (Left Anti Join)
        new_records_df = source_df.join(existing_batches, on="_source_batch_id", how="left_anti")
    else:
        # Table doesn't exist yet, so all records are new
        new_records_df = source_df

    records_to_insert = new_records_df.count()
    print(f"   New records to insert: {records_to_insert}")

    if records_to_insert > 0:
        print("   Appending to fact_player_counts...")

        # Add audit column
        new_records_df = new_records_df.withColumn("_created_at", F.current_timestamp())

        # Append to fact table
        new_records_df.write.format("delta").option("mergeSchema", "true").mode(
            "append"
        ).partitionBy("snapshot_date").save(target_path)

        print(f"   Appended: {records_to_insert} records")
    else:
        print("   No new data to append. Skipping.")

    return TransformResult(
        table_name="fact_player_counts",
        records_processed=total_source_count,
        records_inserted=records_to_insert,
        records_updated=0,
        records_unchanged=total_source_count - records_to_insert,
        success=True,
    )


# =============================================================================
# Main Entry Point
# =============================================================================


def run_silver_transform(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str,
    *,
    include_games: bool = True,
    include_reviews: bool = True,
    include_players: bool = True,
) -> SilverTransformResult:
    """
    Run complete Silver layer transformation.
    """
    started_at = datetime.now()
    results: dict[str, TransformResult] = {}

    print("=" * 60)
    print("SILVER LAYER TRANSFORMATION")
    print("=" * 60)
    print(f"Bronze: {bronze_path}")
    print(f"Silver: {silver_path}")
    print("=" * 60)

    if include_games:
        print("\n" + "-" * 40)
        print("TABLE: dim_games")
        print("-" * 40)
        try:
            results["dim_games"] = transform_games(spark, bronze_path, silver_path)
        except Exception as e:
            print(f"   Error: {e}")
            results["dim_games"] = TransformResult(
                table_name="dim_games",
                records_processed=0,
                records_inserted=0,
                records_updated=0,
                records_unchanged=0,
                success=False,
                error_message=str(e),
            )

    if include_reviews:
        print("\n" + "-" * 40)
        print("TABLE: dim_game_reviews")
        print("-" * 40)
        try:
            results["dim_game_reviews"] = transform_reviews(spark, bronze_path, silver_path)
        except Exception as e:
            print(f"   Error: {e}")
            results["dim_game_reviews"] = TransformResult(
                table_name="dim_game_reviews",
                records_processed=0,
                records_inserted=0,
                records_updated=0,
                records_unchanged=0,
                success=False,
                error_message=str(e),
            )

    if include_players:
        print("\n" + "-" * 40)
        print("TABLE: fact_player_counts")
        print("-" * 40)
        try:
            results["fact_player_counts"] = transform_player_counts(spark, bronze_path, silver_path)
        except Exception as e:
            print(f"   Error: {e}")
            results["fact_player_counts"] = TransformResult(
                table_name="fact_player_counts",
                records_processed=0,
                records_inserted=0,
                records_updated=0,
                records_unchanged=0,
                success=False,
                error_message=str(e),
            )

    completed_at = datetime.now()

    # Print summary
    print("\n" + "=" * 60)
    print("TRANSFORMATION SUMMARY")
    print("=" * 60)
    for table_name, result in results.items():
        status = "SUCCESS" if result.success else "FAILED"
        print(f"[{status}] {table_name}:")
        print(f"      Processed: {result.records_processed}")
        print(f"      Inserted:  {result.records_inserted}")
        print(f"      Updated:   {result.records_updated}")
        if result.error_message:
            print(f"      Error:     {result.error_message}")

    duration = (completed_at - started_at).total_seconds()
    print(f"\nDuration: {duration:.2f} seconds")
    print("=" * 60)

    return SilverTransformResult(
        started_at=started_at,
        completed_at=completed_at,
        results=results,
    )
