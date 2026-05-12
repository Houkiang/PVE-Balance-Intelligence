"""Analyze map danger heatmap metrics and write ADS results."""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze map heatmap metrics from DWS data.")
    parser.add_argument("--input", required=True, help="DWS root path, local or hdfs:/// path.")
    parser.add_argument("--output", required=True, help="ADS output root path, local or hdfs:/// path.")
    parser.add_argument(
        "--display-bin-size",
        type=int,
        default=10,
        help="Display aggregation bin size for 1000x1000 map (default: 10 => 100x100).",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite output dataset.")
    parser.add_argument("--app-name", default="pve-balance-ads-map", help="Spark application name.")
    parser.add_argument("--show-counts", action="store_true", help="Print output row count after writing.")
    return parser.parse_args()


def dataset_path(root: str, name: str) -> str:
    return root.rstrip("/") + f"/{name}"


def read_dataset(spark: SparkSession, root: str, name: str) -> DataFrame:
    return spark.read.parquet(dataset_path(root, name))


def normalize_global(df: DataFrame, col_name: str, output_col: str) -> DataFrame:
    min_max = df.agg(
        F.min(F.col(col_name)).alias("min_v"),
        F.max(F.col(col_name)).alias("max_v"),
    ).first()
    min_v = float(min_max["min_v"]) if min_max["min_v"] is not None else 0.0
    max_v = float(min_max["max_v"]) if min_max["max_v"] is not None else 0.0
    if max_v - min_v <= 1e-9:
        return df.withColumn(output_col, F.lit(0.0))
    return df.withColumn(output_col, (F.col(col_name) - F.lit(min_v)) / F.lit(max_v - min_v))


def build_map_heatmap_report(map_grid_summary: DataFrame, display_bin_size: int) -> DataFrame:
    binned = (
        map_grid_summary.withColumn("display_grid_x", F.floor(F.col("grid_x") / F.lit(display_bin_size)).cast("int"))
        .withColumn("display_grid_y", F.floor(F.col("grid_y") / F.lit(display_bin_size)).cast("int"))
        .groupBy("map_id", "display_grid_x", "display_grid_y")
        .agg(
            F.sum(F.coalesce(F.col("death_count"), F.lit(0))).alias("death_count"),
            F.sum(F.coalesce(F.col("stay_duration"), F.lit(0.0))).alias("stay_duration"),
            F.sum(F.coalesce(F.col("enemy_spawn_count"), F.lit(0))).alias("enemy_spawn_count"),
            F.sum(F.coalesce(F.col("position_tick_count"), F.lit(0))).alias("position_tick_count"),
            F.sum(F.coalesce(F.col("unique_player_count"), F.lit(0))).alias("unique_player_count"),
        )
        .withColumn("dt", F.lit("all"))
    )

    scored = normalize_global(binned, "death_count", "norm_death_count")
    scored = normalize_global(scored, "stay_duration", "norm_stay_duration")
    scored = normalize_global(scored, "enemy_spawn_count", "norm_enemy_spawn_count")

    result = (
        scored.withColumn(
            "danger_score",
            F.lit(0.45) * F.col("norm_death_count")
            + F.lit(0.25) * F.col("norm_stay_duration")
            + F.lit(0.30) * F.col("norm_enemy_spawn_count"),
        )
        .withColumn(
            "danger_level",
            F.when(F.col("danger_score") >= 0.8, F.lit("S"))
            .when(F.col("danger_score") >= 0.6, F.lit("A"))
            .when(F.col("danger_score") >= 0.4, F.lit("B"))
            .when(F.col("danger_score") >= 0.2, F.lit("C"))
            .otherwise(F.lit("D")),
        )
        .withColumn(
            "danger_rank",
            F.dense_rank().over(Window.partitionBy("map_id").orderBy(F.desc("danger_score"))),
        )
        .select(
            "dt",
            "map_id",
            "display_grid_x",
            "display_grid_y",
            "death_count",
            F.round("stay_duration", 4).alias("stay_duration"),
            "enemy_spawn_count",
            "position_tick_count",
            "unique_player_count",
            F.round("danger_score", 6).alias("danger_score"),
            "danger_level",
            "danger_rank",
        )
    )
    return result


def write_dataset(df: DataFrame, root: str, name: str, overwrite: bool) -> None:
    mode = "overwrite" if overwrite else "errorifexists"
    df.write.mode(mode).parquet(dataset_path(root, name))


def main() -> None:
    args = parse_args()
    spark = (
        SparkSession.builder.appName(args.app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        map_grid_summary = read_dataset(spark, args.input, "map_grid_summary")
        report = build_map_heatmap_report(map_grid_summary, args.display_bin_size)

        write_dataset(report, args.output, "map_grid_heatmap_report", args.overwrite)

        if args.show_counts:
            print(f"map_grid_heatmap_report: {report.count()}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
