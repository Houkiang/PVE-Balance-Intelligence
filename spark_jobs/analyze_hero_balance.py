"""Analyze hero balance metrics and write ADS results."""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze hero balance metrics from DWS data.")
    parser.add_argument("--input", required=True, help="DWS root path, local or hdfs:/// path.")
    parser.add_argument("--output", required=True, help="ADS output root path, local or hdfs:/// path.")
    parser.add_argument(
        "--hero-config",
        default="config/hero_config.csv",
        help="Hero configuration CSV path with role and scoring weights.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite output dataset.")
    parser.add_argument("--app-name", default="pve-balance-ads-hero", help="Spark application name.")
    parser.add_argument("--show-counts", action="store_true", help="Print output row count after writing.")
    return parser.parse_args()


def dataset_path(root: str, name: str) -> str:
    return root.rstrip("/") + f"/{name}"


def read_dataset(spark: SparkSession, root: str, name: str) -> DataFrame:
    return spark.read.parquet(dataset_path(root, name))


def resolve_read_path(path: str) -> str:
    if "://" in path:
        return path
    local_path = Path(path)
    if local_path.exists():
        return local_path.resolve().as_uri()
    return path


def normalize_in_window(df: DataFrame, col_name: str, partition_cols: list[str], output_col: str) -> DataFrame:
    window = Window.partitionBy(*partition_cols)
    min_col = F.min(F.col(col_name)).over(window)
    max_col = F.max(F.col(col_name)).over(window)
    return df.withColumn(
        output_col,
        F.when((max_col - min_col) <= F.lit(1e-9), F.lit(0.5)).otherwise((F.col(col_name) - min_col) / (max_col - min_col)),
    )


def normalize_global(df: DataFrame, col_name: str, output_col: str) -> DataFrame:
    min_max = df.agg(
        F.min(F.col(col_name)).alias("min_v"),
        F.max(F.col(col_name)).alias("max_v"),
    ).first()
    min_v = float(min_max["min_v"]) if min_max["min_v"] is not None else 0.0
    max_v = float(min_max["max_v"]) if min_max["max_v"] is not None else 0.0
    if max_v - min_v <= 1e-9:
        return df.withColumn(output_col, F.lit(0.5))
    return df.withColumn(output_col, (F.col(col_name) - F.lit(min_v)) / F.lit(max_v - min_v))


def load_hero_dim(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("header", True)
        .csv(resolve_read_path(path))
        .select(
            F.col("hero_id").cast("string"),
            F.col("hero_name").cast("string"),
            F.col("role_type").cast("string"),
            F.coalesce(F.col("survival_factor").cast("double"), F.lit(1.0)).alias("survival_factor"),
            F.coalesce(F.col("damage_taken_role_weight").cast("double"), F.lit(0.0)).alias("damage_taken_role_weight"),
        )
    )


def build_hero_balance_report(player_match: DataFrame, hero_dim: DataFrame) -> DataFrame:
    base = (
        player_match.select(
            "dt",
            "match_id",
            "player_id",
            "hero_id",
            F.coalesce(F.col("survival_wave").cast("double"), F.lit(0.0)).alias("survival_wave"),
            F.coalesce(F.col("kill_count").cast("double"), F.lit(0.0)).alias("kill_count"),
            F.coalesce(F.col("damage_dealt").cast("double"), F.lit(0.0)).alias("damage_dealt"),
            F.coalesce(F.col("heal_done").cast("double"), F.lit(0.0)).alias("heal_done"),
            F.coalesce(F.col("damage_taken").cast("double"), F.lit(0.0)).alias("damage_taken"),
            F.coalesce(F.col("success_50").cast("int"), F.lit(0)).alias("success_50"),
        )
        .join(hero_dim, ["hero_id"], "left")
        .withColumn("role_type", F.coalesce(F.col("role_type"), F.lit("unknown")))
    )

    score_df = base
    score_df = normalize_in_window(score_df, "damage_dealt", ["role_type"], "norm_damage_dealt")
    score_df = normalize_in_window(score_df, "kill_count", ["role_type"], "norm_kill_count")
    score_df = normalize_in_window(score_df, "heal_done", ["role_type"], "norm_heal_done")
    score_df = normalize_in_window(score_df, "damage_taken", ["role_type"], "norm_damage_taken")
    score_df = normalize_in_window(score_df, "survival_wave", ["role_type"], "norm_survival_wave")

    tank_weight = F.least(F.abs(F.col("damage_taken_role_weight")), F.lit(1.0))
    damage_taken_contrib = F.when(
        F.col("damage_taken_role_weight") >= F.lit(0.0),
        F.col("norm_damage_taken"),
    ).otherwise(F.lit(1.0) - F.col("norm_damage_taken"))

    score_df = score_df.withColumn(
        "role_survival_adjusted_tank_score",
        F.least(
            F.lit(1.0),
            F.greatest(
                F.lit(0.0),
                tank_weight * damage_taken_contrib
                + (F.lit(1.0) - tank_weight) * F.col("norm_survival_wave") * F.col("survival_factor"),
            ),
        ),
    )

    score_df = score_df.withColumn(
        "performance_score",
        F.lit(0.35) * F.col("norm_damage_dealt")
        + F.lit(0.25) * F.col("norm_kill_count")
        + F.lit(0.20) * F.col("norm_heal_done")
        + F.lit(0.20) * F.col("role_survival_adjusted_tank_score"),
    )

    rank_window = Window.partitionBy("hero_id").orderBy(F.desc("performance_score"))
    score_df = score_df.withColumn("performance_percent_rank", F.percent_rank().over(rank_window)).withColumn(
        "is_high_performance",
        (F.col("performance_percent_rank") <= F.lit(0.25)).cast("int"),
    )

    hero_agg = (
        score_df.groupBy("hero_id", "hero_name", "role_type")
        .agg(
            F.count("*").alias("use_count"),
            F.avg("survival_wave").alias("avg_survival_wave"),
            F.sum("kill_count").alias("sum_kill_count"),
            F.sum(F.greatest(F.col("survival_wave"), F.lit(1.0))).alias("sum_survival_wave"),
            F.avg(F.col("success_50").cast("double")).alias("success_50_rate"),
            F.avg(F.col("is_high_performance").cast("double")).alias("high_performance_rate"),
            F.avg("performance_score").alias("avg_performance_score"),
        )
        .withColumn(
            "avg_kill_per_wave",
            F.when(F.col("sum_survival_wave") <= 0.0, F.lit(0.0)).otherwise(F.col("sum_kill_count") / F.col("sum_survival_wave")),
        )
        .drop("sum_kill_count", "sum_survival_wave")
    )

    hero_agg = normalize_global(hero_agg, "success_50_rate", "norm_success_50_rate")
    hero_agg = normalize_global(hero_agg, "avg_survival_wave", "norm_avg_survival_wave")
    hero_agg = normalize_global(hero_agg, "avg_kill_per_wave", "norm_avg_kill_per_wave")
    hero_agg = normalize_global(hero_agg, "high_performance_rate", "norm_high_performance_rate")

    result = (
        hero_agg.withColumn(
            "balance_score",
            F.lit(0.40) * F.col("norm_success_50_rate")
            + F.lit(0.25) * F.col("norm_avg_survival_wave")
            + F.lit(0.20) * F.col("norm_avg_kill_per_wave")
            + F.lit(0.15) * F.col("norm_high_performance_rate"),
        )
        .withColumn("dt", F.lit("all"))
        .withColumn("balance_rank", F.dense_rank().over(Window.orderBy(F.desc("balance_score"))))
        .select(
            "dt",
            "hero_id",
            "hero_name",
            "role_type",
            "use_count",
            F.round("avg_survival_wave", 4).alias("avg_survival_wave"),
            F.round("avg_kill_per_wave", 4).alias("avg_kill_per_wave"),
            F.round("success_50_rate", 6).alias("success_50_rate"),
            F.round("high_performance_rate", 6).alias("high_performance_rate"),
            F.round("avg_performance_score", 6).alias("avg_performance_score"),
            F.round("balance_score", 6).alias("balance_score"),
            "balance_rank",
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
        player_match = read_dataset(spark, args.input, "player_match_summary")
        hero_dim = load_hero_dim(spark, args.hero_config)
        report = build_hero_balance_report(player_match, hero_dim)

        write_dataset(report, args.output, "hero_balance_report", args.overwrite)

        if args.show_counts:
            print(f"hero_balance_report: {report.count()}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
