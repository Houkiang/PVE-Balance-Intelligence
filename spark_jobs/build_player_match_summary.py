"""Build DWS match and player-match summary datasets."""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build DWS summary datasets from DWD Parquet data.")
    parser.add_argument("--input", required=True, help="DWD root path, local or hdfs:/// path.")
    parser.add_argument("--output", required=True, help="DWS output root path, local or hdfs:/// path.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing output datasets.")
    parser.add_argument("--show-counts", action="store_true", help="Print output row counts after writing.")
    parser.add_argument("--target-wave", type=int, default=50, help="Wave threshold for match success.")
    parser.add_argument(
        "--output-partitions",
        type=int,
        default=2,
        help="Number of partitions before writing each DWS dataset. Increase for large data.",
    )
    parser.add_argument(
        "--position-tick-seconds",
        type=float,
        default=3.0,
        help="Seconds represented by one position_tick event.",
    )
    parser.add_argument("--app-name", default="pve-balance-build-dws", help="Spark application name.")
    return parser.parse_args()


def dataset_path(root: str, name: str) -> str:
    return root.rstrip("/") + f"/{name}"


def read_dataset(spark: SparkSession, root: str, name: str) -> DataFrame:
    return spark.read.parquet(dataset_path(root, name))


def write_dataset(df: DataFrame, root: str, name: str, overwrite: bool, output_partitions: int) -> None:
    mode = "overwrite" if overwrite else "errorifexists"
    writer_df = df.coalesce(output_partitions) if output_partitions > 0 else df
    writer_df.write.mode(mode).partitionBy("dt").parquet(dataset_path(root, name))


def build_match_summary(
    game_event: DataFrame,
    player_event: DataFrame,
    target_wave: int,
) -> DataFrame:
    hero_sets = (
        player_event.filter(F.col("event_type") == "player_join")
        .groupBy("dt", "match_id")
        .agg(
            F.countDistinct("player_id").alias("team_size_from_join"),
            F.sort_array(F.collect_list("hero_id")).alias("team_heroes_from_join"),
            F.sort_array(F.collect_set("hero_id")).alias("hero_set"),
        )
    )

    team_heroes_schema = T.ArrayType(T.StringType())
    battle_end = (
        game_event.filter(F.col("event_type") == "battle_end")
        .select(
            "dt",
            "match_id",
            "room_id",
            "map_id",
            F.get_json_object("extra", "$.final_wave").cast("int").alias("final_wave"),
            F.get_json_object("extra", "$.success_50").cast("boolean").alias("success_50_raw"),
            F.get_json_object("extra", "$.team_size").cast("int").alias("team_size_from_end"),
            F.get_json_object("extra", "$.survivor_count").cast("int").alias("survivor_count"),
            F.from_json(F.get_json_object("extra", "$.team_heroes"), team_heroes_schema).alias("team_heroes_from_end"),
        )
    )

    return (
        battle_end.join(hero_sets, ["dt", "match_id"], "left")
        .withColumn("team_size", F.coalesce("team_size_from_end", "team_size_from_join"))
        .withColumn("team_heroes", F.coalesce("team_heroes_from_end", "team_heroes_from_join"))
        .withColumn("success_50", F.coalesce("success_50_raw", F.col("final_wave") >= F.lit(target_wave)))
        .select(
            "dt",
            "match_id",
            "room_id",
            "map_id",
            "final_wave",
            "success_50",
            "team_size",
            "survivor_count",
            "team_heroes",
            "hero_set",
        )
    )


def build_player_card_set(card_pick: DataFrame) -> DataFrame:
    return (
        card_pick.filter((F.col("event_type") == "card_pick") & F.col("card_id").isNotNull())
        .groupBy("dt", "match_id", "player_id", "hero_id")
        .agg(
            F.sort_array(F.collect_set("card_id")).alias("card_set"),
            F.sort_array(F.collect_set("card_type")).alias("card_type_set"),
            F.sort_array(F.collect_set("target_weapon")).alias("weapon_set"),
            F.sort_array(F.collect_set("build_tags")).alias("build_tag_set"),
            F.count("*").alias("picked_card_count"),
        )
    )


def build_player_match_summary(
    match_summary: DataFrame,
    player_event: DataFrame,
    player_wave: DataFrame,
    player_card_set: DataFrame,
) -> DataFrame:
    joins = (
        player_event.filter(F.col("event_type") == "player_join")
        .select("dt", "match_id", "room_id", "map_id", "player_id", "hero_id")
        .dropDuplicates(["dt", "match_id", "player_id"])
    )

    death = (
        player_event.filter(F.col("event_type") == "player_death")
        .groupBy("dt", "match_id", "player_id")
        .agg(
            F.min("death_wave").alias("death_wave"),
            F.first("grid_id", ignorenulls=True).alias("death_grid_id"),
            F.first("grid_x", ignorenulls=True).alias("death_grid_x"),
            F.first("grid_y", ignorenulls=True).alias("death_grid_y"),
        )
    )

    wave_summary = (
        player_wave.groupBy("dt", "match_id", "player_id")
        .agg(
            F.max("wave").alias("max_active_wave"),
            F.sum("damage_dealt").alias("damage_dealt"),
            F.sum("kill_count").cast("int").alias("kill_count"),
            F.sum("heal_done").alias("heal_done"),
            F.sum("damage_taken").alias("damage_taken"),
            F.max("weapon_count").alias("weapon_count"),
            F.max("card_count").alias("card_count"),
            F.min("hp_after_wave").alias("min_hp_after_wave"),
            F.avg("hp_after_wave").alias("avg_hp_after_wave"),
        )
    )

    return (
        joins.join(match_summary.select("dt", "match_id", "final_wave", "success_50"), ["dt", "match_id"], "left")
        .join(death, ["dt", "match_id", "player_id"], "left")
        .join(wave_summary, ["dt", "match_id", "player_id"], "left")
        .join(
            player_card_set.select("dt", "match_id", "player_id", "card_set", "picked_card_count"),
            ["dt", "match_id", "player_id"],
            "left",
        )
        .withColumn("survival_wave", F.coalesce("death_wave", "max_active_wave", "final_wave"))
        .withColumn("is_dead", F.col("death_wave").isNotNull())
        .withColumn("damage_dealt", F.coalesce("damage_dealt", F.lit(0.0)))
        .withColumn("kill_count", F.coalesce("kill_count", F.lit(0)))
        .withColumn("heal_done", F.coalesce("heal_done", F.lit(0.0)))
        .withColumn("damage_taken", F.coalesce("damage_taken", F.lit(0.0)))
        .withColumn("weapon_count", F.coalesce("weapon_count", F.lit(0)))
        .withColumn("card_count", F.coalesce("card_count", F.lit(0)))
        .withColumn("picked_card_count", F.coalesce("picked_card_count", F.lit(0)))
        .withColumn("card_set", F.coalesce("card_set", F.array().cast(T.ArrayType(T.StringType()))))
        .select(
            "dt",
            "match_id",
            "room_id",
            "map_id",
            "player_id",
            "hero_id",
            "final_wave",
            "success_50",
            "survival_wave",
            "is_dead",
            "death_wave",
            "death_grid_id",
            "death_grid_x",
            "death_grid_y",
            "damage_dealt",
            "kill_count",
            "heal_done",
            "damage_taken",
            "weapon_count",
            "card_count",
            "picked_card_count",
            "card_set",
            "min_hp_after_wave",
            "avg_hp_after_wave",
        )
    )


def build_map_grid_summary(
    player_event: DataFrame,
    position_tick: DataFrame,
    enemy_spawn: DataFrame,
    position_tick_seconds: float,
) -> DataFrame:
    deaths = (
        player_event.filter(F.col("event_type") == "player_death")
        .groupBy("dt", "map_id", "grid_id", "grid_x", "grid_y")
        .agg(F.count("*").alias("death_count"))
    )

    stays = (
        position_tick.groupBy("dt", "map_id", "grid_id", "grid_x", "grid_y")
        .agg(
            F.count("*").alias("position_tick_count"),
            F.countDistinct("player_id").alias("unique_player_count"),
        )
        .withColumn("stay_duration", F.col("position_tick_count") * F.lit(position_tick_seconds))
    )

    spawns = (
        enemy_spawn.groupBy("dt", "map_id", "grid_id", "grid_x", "grid_y")
        .agg(
            F.sum("enemy_count").cast("int").alias("enemy_spawn_count"),
            F.count("*").alias("enemy_spawn_events"),
        )
    )

    keys = ["dt", "map_id", "grid_id", "grid_x", "grid_y"]
    return (
        stays.join(deaths, keys, "full")
        .join(spawns, keys, "full")
        .withColumn("position_tick_count", F.coalesce("position_tick_count", F.lit(0)))
        .withColumn("unique_player_count", F.coalesce("unique_player_count", F.lit(0)))
        .withColumn("stay_duration", F.coalesce("stay_duration", F.lit(0.0)))
        .withColumn("death_count", F.coalesce("death_count", F.lit(0)))
        .withColumn("enemy_spawn_count", F.coalesce("enemy_spawn_count", F.lit(0)))
        .withColumn("enemy_spawn_events", F.coalesce("enemy_spawn_events", F.lit(0)))
        .select(
            "dt",
            "map_id",
            "grid_id",
            "grid_x",
            "grid_y",
            "death_count",
            "position_tick_count",
            "unique_player_count",
            "stay_duration",
            "enemy_spawn_count",
            "enemy_spawn_events",
        )
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
        game_event = read_dataset(spark, args.input, "game_event_detail")
        player_event = read_dataset(spark, args.input, "player_event_detail")
        player_wave = read_dataset(spark, args.input, "player_wave_stat_detail")
        card_pick = read_dataset(spark, args.input, "card_pick_detail")
        position_tick = read_dataset(spark, args.input, "position_tick_detail")
        enemy_spawn = read_dataset(spark, args.input, "enemy_spawn_detail")

        match_summary = build_match_summary(game_event, player_event, args.target_wave).cache()
        player_card_set = build_player_card_set(card_pick).cache()
        player_match_summary = build_player_match_summary(
            match_summary,
            player_event,
            player_wave,
            player_card_set,
        )
        map_grid_summary = build_map_grid_summary(
            player_event,
            position_tick,
            enemy_spawn,
            args.position_tick_seconds,
        )

        datasets = {
            "match_summary": match_summary,
            "player_match_summary": player_match_summary,
            "player_card_set": player_card_set,
            "map_grid_summary": map_grid_summary,
        }

        for name, df in datasets.items():
            write_dataset(df, args.output, name, args.overwrite, args.output_partitions)

        if args.show_counts:
            for name, df in datasets.items():
                print(f"{name}: {df.count()}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
