"""Clean ODS JSON Lines events into DWD Parquet datasets."""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


EVENT_TYPES_FOR_PLAYER_DETAIL = ["player_join", "player_death", "level_up"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean ODS game events into DWD Parquet datasets.")
    parser.add_argument(
        "--input",
        required=True,
        help="Input ODS path. Supports local path or hdfs:/// path. Root may contain dt=* partitions.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output DWD root path. Datasets are written below this root.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing output datasets.")
    parser.add_argument("--app-name", default="pve-balance-clean-events", help="Spark application name.")
    parser.add_argument("--show-counts", action="store_true", help="Print output row counts after writing.")
    return parser.parse_args()


def is_remote_path(path: str) -> bool:
    return "://" in path


def input_pattern(path: str) -> str:
    if "*" in path or path.endswith(".jsonl") or path.endswith(".json"):
        return path
    if is_remote_path(path):
        return path.rstrip("/") + "/dt=*/events.jsonl"

    local_path = Path(path)
    if local_path.is_file():
        return local_path.as_posix()
    direct_file = local_path / "events.jsonl"
    if direct_file.exists():
        return direct_file.as_posix()
    return (local_path / "dt=*" / "events.jsonl").as_posix()


def clean_base_events(raw: DataFrame) -> DataFrame:
    x_col = F.greatest(F.lit(0.0), F.least(F.col("x").cast("double"), F.lit(999.0)))
    y_col = F.greatest(F.lit(0.0), F.least(F.col("y").cast("double"), F.lit(999.0)))

    return (
        raw.select(
            F.col("event_id").cast("string").alias("event_id"),
            F.col("event_type").cast("string").alias("event_type"),
            F.to_timestamp("event_time").alias("event_time"),
            F.col("dt").cast("string").alias("dt"),
            F.col("match_id").cast("string").alias("match_id"),
            F.col("room_id").cast("string").alias("room_id"),
            F.col("map_id").cast("string").alias("map_id"),
            F.when(F.length(F.col("player_id")) > 0, F.col("player_id")).otherwise(F.lit(None)).alias("player_id"),
            F.when(F.length(F.col("hero_id")) > 0, F.col("hero_id")).otherwise(F.lit(None)).alias("hero_id"),
            F.col("wave").cast("int").alias("wave"),
            x_col.alias("x"),
            y_col.alias("y"),
            F.col("extra").cast("string").alias("extra"),
        )
        .filter(
            F.col("event_id").isNotNull()
            & F.col("event_type").isNotNull()
            & F.col("event_time").isNotNull()
            & F.col("dt").isNotNull()
            & F.col("match_id").isNotNull()
        )
        .withColumn("grid_x", F.floor("x").cast("int"))
        .withColumn("grid_y", F.floor("y").cast("int"))
        .withColumn("grid_id", F.concat_ws("_", F.col("map_id"), F.col("grid_x"), F.col("grid_y")))
    )


def player_wave_stat_detail(base: DataFrame) -> DataFrame:
    return base.filter(F.col("event_type") == "player_wave_stat").select(
        "event_id",
        "event_time",
        "dt",
        "match_id",
        "room_id",
        "map_id",
        "player_id",
        "hero_id",
        "wave",
        "x",
        "y",
        "grid_x",
        "grid_y",
        "grid_id",
        F.get_json_object("extra", "$.damage_dealt").cast("double").alias("damage_dealt"),
        F.get_json_object("extra", "$.kill_count").cast("int").alias("kill_count"),
        F.get_json_object("extra", "$.heal_done").cast("double").alias("heal_done"),
        F.get_json_object("extra", "$.damage_taken").cast("double").alias("damage_taken"),
        F.get_json_object("extra", "$.weapon_count").cast("int").alias("weapon_count"),
        F.get_json_object("extra", "$.card_count").cast("int").alias("card_count"),
        F.get_json_object("extra", "$.hp_after_wave").cast("double").alias("hp_after_wave"),
    )


def card_pick_detail(base: DataFrame) -> DataFrame:
    candidate_schema = T.ArrayType(T.StringType())
    return base.filter(F.col("event_type").isin("card_choice", "card_pick")).select(
        "event_id",
        "event_type",
        "event_time",
        "dt",
        "match_id",
        "room_id",
        "map_id",
        "player_id",
        "hero_id",
        "wave",
        F.from_json(F.get_json_object("extra", "$.candidate_card_ids"), candidate_schema).alias("candidate_card_ids"),
        F.get_json_object("extra", "$.card_id").alias("card_id"),
        F.get_json_object("extra", "$.card_type").alias("card_type"),
        F.get_json_object("extra", "$.target_weapon").alias("target_weapon"),
        F.get_json_object("extra", "$.build_tags").alias("build_tags"),
        F.get_json_object("extra", "$.reason").alias("reason"),
    )


def position_tick_detail(base: DataFrame) -> DataFrame:
    return base.filter(F.col("event_type") == "position_tick").select(
        "event_id",
        "event_time",
        "dt",
        "match_id",
        "room_id",
        "map_id",
        "player_id",
        "hero_id",
        "wave",
        "x",
        "y",
        "grid_x",
        "grid_y",
        "grid_id",
    )


def enemy_spawn_detail(base: DataFrame) -> DataFrame:
    return base.filter(F.col("event_type") == "enemy_spawn").select(
        "event_id",
        "event_time",
        "dt",
        "match_id",
        "room_id",
        "map_id",
        "wave",
        "x",
        "y",
        "grid_x",
        "grid_y",
        "grid_id",
        F.get_json_object("extra", "$.enemy_count").cast("int").alias("enemy_count"),
        F.get_json_object("extra", "$.danger_zone").alias("danger_zone"),
    )


def player_event_detail(base: DataFrame) -> DataFrame:
    return base.filter(F.col("event_type").isin(EVENT_TYPES_FOR_PLAYER_DETAIL)).select(
        "event_id",
        "event_type",
        "event_time",
        "dt",
        "match_id",
        "room_id",
        "map_id",
        "player_id",
        "hero_id",
        "wave",
        "x",
        "y",
        "grid_x",
        "grid_y",
        "grid_id",
        F.get_json_object("extra", "$.death_wave").cast("int").alias("death_wave"),
        F.get_json_object("extra", "$.hp").cast("double").alias("hp"),
    )


def write_dataset(df: DataFrame, root: str, name: str, overwrite: bool) -> None:
    mode = "overwrite" if overwrite else "errorifexists"
    path = root.rstrip("/") + f"/{name}"
    (
        df.write.mode(mode)
        .partitionBy("dt")
        .parquet(path)
    )


def main() -> None:
    args = parse_args()
    spark = (
        SparkSession.builder.appName(args.app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        raw = spark.read.json(input_pattern(args.input))
        base = clean_base_events(raw).cache()

        datasets = {
            "game_event_detail": base,
            "player_event_detail": player_event_detail(base),
            "player_wave_stat_detail": player_wave_stat_detail(base),
            "card_pick_detail": card_pick_detail(base),
            "position_tick_detail": position_tick_detail(base),
            "enemy_spawn_detail": enemy_spawn_detail(base),
        }

        for name, df in datasets.items():
            write_dataset(df, args.output, name, args.overwrite)

        if args.show_counts:
            for name, df in datasets.items():
                print(f"{name}: {df.count()}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
