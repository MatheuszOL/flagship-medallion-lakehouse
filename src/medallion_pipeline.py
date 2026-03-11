from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    concat_ws,
    count,
    current_timestamp,
    date_format,
    lit,
    sha2,
    sum as spark_sum,
    to_timestamp,
    when,
)

from privacy import anonymize_columns


DEFAULT_SENSITIVE_COLUMNS = ["email", "cpf", "ssn", "phone", "card_number"]
DEFAULT_REQUIRED_COLUMNS = ["tpep_pickup_datetime", "trip_distance"]


def create_spark(app_name: str = "FlagshipMedallionLakehouse") -> SparkSession:
    # Sessão Spark com Delta habilitado. Mesmo padrão usado em projetos orientados a Databricks.
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def read_raw_input(spark: SparkSession, input_path: str, input_format: str) -> DataFrame:
    reader = spark.read
    if input_format == "parquet":
        return reader.parquet(input_path)
    if input_format == "csv":
        return reader.option("header", True).option("inferSchema", True).csv(input_path)
    if input_format == "json":
        return reader.json(input_path)
    raise ValueError(f"Unsupported input format: {input_format}")


def write_delta(
    df: DataFrame,
    output_path: str,
    mode: str = "overwrite",
    partition_by: str | None = None,
    merge_schema: bool = False,
) -> None:
    writer = df.write.format("delta").mode(mode)
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    if partition_by and partition_by in df.columns:
        writer = writer.partitionBy(partition_by)
    writer.save(output_path)


def _path_exists(path: str) -> bool:
    return Path(path).exists()


def _load_processed_batches(state_path: str) -> set[str]:
    file_path = Path(state_path)
    if not file_path.exists():
        return set()
    return {line.strip() for line in file_path.read_text(encoding="utf-8").splitlines() if line.strip()}


def _register_processed_batch(state_path: str, batch_id: str) -> None:
    file_path = Path(state_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("a", encoding="utf-8") as handle:
        handle.write(f"{batch_id}\n")


def _detect_new_columns(df: DataFrame, schema_state_path: str) -> list[str]:
    schema_file = Path(schema_state_path)
    known_columns: set[str] = set()
    if schema_file.exists():
        known_columns = {line.strip() for line in schema_file.read_text(encoding="utf-8").splitlines() if line.strip()}

    incoming_columns = set(df.columns)
    new_columns = sorted(incoming_columns - known_columns) if known_columns else []

    schema_file.parent.mkdir(parents=True, exist_ok=True)
    schema_file.write_text("\n".join(sorted(incoming_columns)), encoding="utf-8")
    return new_columns


def _with_trip_id(df: DataFrame) -> DataFrame:
    if "trip_id" in df.columns:
        return df

    candidate_keys = [key for key in ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance"] if key in df.columns]
    if candidate_keys:
        return df.withColumn("trip_id", sha2(concat_ws("||", *[col(key).cast("string") for key in candidate_keys]), 256))

    # I keep this fallback so the pipeline still runs even when the source is messy and missing standard taxi columns.
    return df.withColumn("trip_id", sha2(concat_ws("||", *[col(column_name).cast("string") for column_name in df.columns]), 256))


def build_bronze(raw_df: DataFrame, bronze_path: str, batch_id: str) -> DataFrame:
    bronze_df = (
        raw_df.withColumn("ingest_batch_id", lit(batch_id))
        .withColumn("ingested_at", current_timestamp())
    )

    write_mode = "append" if _path_exists(bronze_path) else "overwrite"
    # I intentionally keep mergeSchema enabled on bronze to absorb schema drift instead of breaking the whole run.
    write_delta(bronze_df, bronze_path, mode=write_mode, merge_schema=True)
    return bronze_df


def build_silver(
    spark: SparkSession,
    bronze_batch_df: DataFrame,
    sensitive_columns: list[str],
    required_columns: list[str],
    silver_path: str,
) -> DataFrame:
    df = bronze_batch_df

    if "tpep_pickup_datetime" in df.columns:
        df = df.withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))

    if "tpep_dropoff_datetime" in df.columns:
        df = df.withColumn("tpep_dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))

    existing_required = [column_name for column_name in required_columns if column_name in df.columns]
    if existing_required:
        df = df.dropna(subset=existing_required)

    df = _with_trip_id(df)
    df = df.dropDuplicates(["trip_id"])

    df = anonymize_columns(df, sensitive_columns)
    df = df.withColumn("record_last_update_ts", current_timestamp())

    if "tpep_pickup_datetime" in df.columns:
        df = df.withColumn("pickup_date", date_format(col("tpep_pickup_datetime"), "yyyy-MM-dd"))

    if not _path_exists(silver_path):
        write_delta(df, silver_path, mode="overwrite", partition_by="pickup_date", merge_schema=True)
    else:
        # I use merge here because late updates are common; append-only would duplicate trips over time.
        delta_target = DeltaTable.forPath(spark, silver_path)
        (
            delta_target.alias("target")
            .merge(df.alias("source"), "target.trip_id = source.trip_id")
            .whenMatchedUpdateAll(condition="source.record_last_update_ts >= target.record_last_update_ts")
            .whenNotMatchedInsertAll()
            .execute()
        )

    return spark.read.format("delta").load(silver_path)


def build_quality_report(
    spark: SparkSession,
    raw_batch_df: DataFrame,
    silver_batch_df: DataFrame,
    required_columns: list[str],
    batch_id: str,
    quality_report_path: str,
    new_columns_detected: list[str],
) -> DataFrame:
    total_rows = raw_batch_df.count()
    invalid_rows = 0

    existing_required = [column_name for column_name in required_columns if column_name in raw_batch_df.columns]
    if existing_required:
        invalid_expr = None
        for required_column in existing_required:
            condition = col(required_column).isNull()
            invalid_expr = condition if invalid_expr is None else (invalid_expr | condition)
        invalid_rows = raw_batch_df.filter(invalid_expr).count()

    raw_with_trip = _with_trip_id(raw_batch_df)
    unique_trip_rows = raw_with_trip.select("trip_id").dropDuplicates().count()
    duplicate_rows = max(total_rows - unique_trip_rows, 0)
    duplicate_ratio = duplicate_rows / total_rows if total_rows else 0.0
    invalid_ratio = invalid_rows / total_rows if total_rows else 0.0

    report_df = spark.createDataFrame(
        [
            {
                "batch_id": batch_id,
                "processed_at": datetime.utcnow().isoformat(),
                "raw_rows": int(total_rows),
                "silver_rows_after_merge": int(silver_batch_df.count()),
                "invalid_rows_count": int(invalid_rows),
                "invalid_ratio": float(invalid_ratio),
                "duplicate_rows_count": int(duplicate_rows),
                "duplicate_ratio": float(duplicate_ratio),
                "new_columns_detected": ",".join(new_columns_detected) if new_columns_detected else "",
            }
        ]
    )

    write_mode = "append" if _path_exists(quality_report_path) else "overwrite"
    write_delta(report_df, quality_report_path, mode=write_mode)
    return report_df


def build_gold(silver_df: DataFrame, gold_path: str) -> tuple[DataFrame, DataFrame]:
    region_column = "region_id" if "region_id" in silver_df.columns else ("VendorID" if "VendorID" in silver_df.columns else None)
    segment_column = "customer_segment" if "customer_segment" in silver_df.columns else ("VendorID" if "VendorID" in silver_df.columns else None)

    revenue_group = []
    if "pickup_date" in silver_df.columns:
        revenue_group.append("pickup_date")
    if region_column:
        revenue_group.append(region_column)
    revenue_agg = []
    if "total_amount" in silver_df.columns:
        revenue_agg.append(spark_sum("total_amount").alias("total_revenue"))
    revenue_agg.append(count("*").alias("total_trips"))

    if revenue_group:
        revenue_df = silver_df.groupBy(*revenue_group).agg(*revenue_agg)
    else:
        revenue_df = silver_df.agg(*revenue_agg)

    trips_by_segment = silver_df
    segment_group = []
    if "pickup_date" in silver_df.columns:
        segment_group.append("pickup_date")
    if segment_column:
        segment_group.append(segment_column)
    if segment_group:
        trips_by_segment = trips_by_segment.groupBy(*segment_group)
    else:
        trips_by_segment = trips_by_segment.groupBy()

    segment_agg = [count("*").alias("trips_count")]
    if "trip_distance" in silver_df.columns:
        segment_agg.append(avg("trip_distance").alias("avg_trip_distance"))
    segment_df = trips_by_segment.agg(*segment_agg)

    write_delta(revenue_df, str(Path(gold_path) / "revenue_by_region"), mode="overwrite")
    write_delta(segment_df, str(Path(gold_path) / "trips_by_customer_segment"), mode="overwrite")
    return revenue_df, segment_df


def run_pipeline(args: argparse.Namespace) -> None:
    spark = create_spark()

    batch_id = args.batch_id or Path(args.input_path).stem
    already_processed = _load_processed_batches(args.incremental_state_path)
    if batch_id in already_processed:
        print(f"Batch '{batch_id}' already processed. Skipping this run to keep idempotent behavior.")
        spark.stop()
        return

    raw_df = read_raw_input(spark, args.input_path, args.input_format)
    new_columns = _detect_new_columns(raw_df, args.schema_state_path)
    if new_columns:
        print(f"Schema evolution detected. New columns in this batch: {', '.join(new_columns)}")

    bronze_df = build_bronze(raw_df, args.bronze_path, batch_id)

    sensitive_columns = [col_name.strip() for col_name in args.sensitive_columns.split(",") if col_name.strip()]
    required_columns = [col_name.strip() for col_name in args.required_columns.split(",") if col_name.strip()]

    bronze_batch_df = bronze_df.filter(col("ingest_batch_id") == lit(batch_id))
    silver_df = build_silver(spark, bronze_batch_df, sensitive_columns, required_columns, args.silver_path)
    quality_df = build_quality_report(
        spark,
        raw_batch_df=raw_df,
        silver_batch_df=silver_df,
        required_columns=required_columns,
        batch_id=batch_id,
        quality_report_path=args.quality_report_path,
        new_columns_detected=new_columns,
    )
    revenue_df, segment_df = build_gold(silver_df, args.gold_path)

    _register_processed_batch(args.incremental_state_path, batch_id)

    print("Pipeline execution finished.")
    print(f"Batch id: {batch_id}")
    print(f"Bronze rows: {bronze_batch_df.count()}")
    print(f"Silver rows: {silver_df.count()}")
    print(f"Gold revenue_by_region rows: {revenue_df.count()}")
    print(f"Gold trips_by_customer_segment rows: {segment_df.count()}")
    print(f"Quality report rows appended: {quality_df.count()}")

    spark.stop()


def parse_args() -> argparse.Namespace:
    project_root = Path(__file__).resolve().parents[1]

    parser = argparse.ArgumentParser(description="End-to-end Medallion Lakehouse pipeline with Delta Lake.")
    parser.add_argument("--input-path", required=True, help="Raw dataset path (local or cloud URI).")
    parser.add_argument("--input-format", default="parquet", choices=["parquet", "csv", "json"], help="Input file format.")
    parser.add_argument("--bronze-path", default=str(project_root / "data" / "bronze"), help="Bronze Delta output path.")
    parser.add_argument("--silver-path", default=str(project_root / "data" / "silver"), help="Silver Delta output path.")
    parser.add_argument("--gold-path", default=str(project_root / "data" / "gold"), help="Gold Delta output path.")
    parser.add_argument(
        "--quality-report-path",
        default=str(project_root / "reports" / "data_quality_report"),
        help="Delta path where quality metrics per batch are stored.",
    )
    parser.add_argument(
        "--incremental-state-path",
        default=str(project_root / "reports" / "processed_batches.txt"),
        help="Text file used to track which batch ids were already processed.",
    )
    parser.add_argument(
        "--schema-state-path",
        default=str(project_root / "reports" / "bronze_schema_columns.txt"),
        help="Text file used to compare incoming schema and detect new columns.",
    )
    parser.add_argument(
        "--batch-id",
        default=None,
        help="Optional explicit batch identifier. If omitted, input filename stem is used.",
    )
    parser.add_argument("--sensitive-columns", default=",".join(DEFAULT_SENSITIVE_COLUMNS), help="Comma-separated sensitive columns.")
    parser.add_argument("--required-columns", default=",".join(DEFAULT_REQUIRED_COLUMNS), help="Columns used for null filtering.")
    return parser.parse_args()


if __name__ == "__main__":
    run_pipeline(parse_args())
