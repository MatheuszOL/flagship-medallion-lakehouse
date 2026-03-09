from __future__ import annotations

import argparse
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, date_format, sum as spark_sum, avg, to_timestamp

from privacy import anonymize_columns


DEFAULT_SENSITIVE_COLUMNS = ["email", "cpf", "ssn", "phone", "card_number"]
DEFAULT_REQUIRED_COLUMNS = ["tpep_pickup_datetime", "trip_distance"]


def create_spark(app_name: str = "FlagshipMedallionLakehouse") -> SparkSession:
    # Delta-enabled Spark session. Same pattern used in Databricks-oriented projects.
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


def write_delta(df: DataFrame, output_path: str, mode: str = "overwrite", partition_by: str | None = None) -> None:
    writer = df.write.format("delta").mode(mode)
    if partition_by and partition_by in df.columns:
        writer = writer.partitionBy(partition_by)
    writer.save(output_path)


def build_bronze(raw_df: DataFrame, bronze_path: str) -> DataFrame:
    # Bronze = raw persistence. No heavy transformation here by design.
    bronze_df = raw_df
    write_delta(bronze_df, bronze_path)
    return bronze_df


def build_silver(bronze_df: DataFrame, sensitive_columns: list[str], required_columns: list[str], silver_path: str) -> DataFrame:
    # Silver = enforce quality + standardize schema + anonymize sensitive fields.
    df = bronze_df

    if "tpep_pickup_datetime" in df.columns:
        df = df.withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))

    if "tpep_dropoff_datetime" in df.columns:
        df = df.withColumn("tpep_dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))

    existing_required = [column_name for column_name in required_columns if column_name in df.columns]
    if existing_required:
        df = df.dropna(subset=existing_required)

    if "VendorID" in df.columns and "tpep_pickup_datetime" in df.columns:
        # Practical dedup key for taxi-like datasets.
        df = df.dropDuplicates(["VendorID", "tpep_pickup_datetime", "trip_distance"])
    else:
        df = df.dropDuplicates()

    df = anonymize_columns(df, sensitive_columns)

    if "tpep_pickup_datetime" in df.columns:
        df = df.withColumn("pickup_date", date_format(col("tpep_pickup_datetime"), "yyyy-MM-dd"))

    write_delta(df, silver_path, partition_by="pickup_date")
    return df


def build_gold(silver_df: DataFrame, gold_path: str) -> DataFrame:
    # Gold = business-facing KPI layer.
    metrics = []

    if "trip_distance" in silver_df.columns:
        metrics.append(avg("trip_distance").alias("avg_trip_distance"))

    if "total_amount" in silver_df.columns:
        metrics.append(spark_sum("total_amount").alias("total_revenue"))

    metrics.append(count("*").alias("total_trips"))

    if "pickup_date" in silver_df.columns:
        group_columns = ["pickup_date"]
    else:
        group_columns = []

    if group_columns:
        gold_df = silver_df.groupBy(*group_columns).agg(*metrics).orderBy(*group_columns)
    else:
        gold_df = silver_df.agg(*metrics)

    write_delta(gold_df, gold_path)
    return gold_df


def run_pipeline(args: argparse.Namespace) -> None:
    spark = create_spark()

    raw_df = read_raw_input(spark, args.input_path, args.input_format)
    bronze_df = build_bronze(raw_df, args.bronze_path)

    sensitive_columns = [col_name.strip() for col_name in args.sensitive_columns.split(",") if col_name.strip()]
    required_columns = [col_name.strip() for col_name in args.required_columns.split(",") if col_name.strip()]

    silver_df = build_silver(bronze_df, sensitive_columns, required_columns, args.silver_path)
    gold_df = build_gold(silver_df, args.gold_path)

    print("Pipeline execution finished.")
    print(f"Bronze rows: {bronze_df.count()}")
    print(f"Silver rows: {silver_df.count()}")
    print(f"Gold rows: {gold_df.count()}")

    spark.stop()


def parse_args() -> argparse.Namespace:
    project_root = Path(__file__).resolve().parents[1]

    parser = argparse.ArgumentParser(description="End-to-end Medallion Lakehouse pipeline with Delta Lake.")
    parser.add_argument("--input-path", required=True, help="Raw dataset path (local or cloud URI).")
    parser.add_argument("--input-format", default="parquet", choices=["parquet", "csv", "json"], help="Input file format.")
    parser.add_argument("--bronze-path", default=str(project_root / "data" / "bronze"), help="Bronze Delta output path.")
    parser.add_argument("--silver-path", default=str(project_root / "data" / "silver"), help="Silver Delta output path.")
    parser.add_argument("--gold-path", default=str(project_root / "data" / "gold"), help="Gold Delta output path.")
    parser.add_argument("--sensitive-columns", default=",".join(DEFAULT_SENSITIVE_COLUMNS), help="Comma-separated sensitive columns.")
    parser.add_argument("--required-columns", default=",".join(DEFAULT_REQUIRED_COLUMNS), help="Columns used for null filtering.")
    return parser.parse_args()


if __name__ == "__main__":
    run_pipeline(parse_args())
