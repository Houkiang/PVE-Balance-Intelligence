"""Analyze card strength metrics and write ADS results."""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze card strength from DWD + DWS data.")
    parser.add_argument("--dwd-input", required=True, help="DWD root path, local or hdfs:/// path.")
    parser.add_argument("--dws-input", required=True, help="DWS root path, local or hdfs:/// path.")
    parser.add_argument("--output", required=True, help="ADS output root path, local or hdfs:/// path.")
    parser.add_argument("--card-config", default="config/card_config.csv", help="Card config CSV path.")
    parser.add_argument("--weapon-config", default="config/weapon_config.csv", help="Weapon config CSV path.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite output dataset.")
    parser.add_argument("--app-name", default="pve-balance-ads-card", help="Spark application name.")
    parser.add_argument("--show-counts", action="store_true", help="Print output row counts after writing.")
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


def write_dataset(df: DataFrame, root: str, name: str, overwrite: bool) -> None:
    mode = "overwrite" if overwrite else "errorifexists"
    df.write.mode(mode).parquet(dataset_path(root, name))


def load_card_dim(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("header", True)
        .csv(resolve_read_path(path))
        .select(
            F.col("card_id").cast("string"),
            F.col("card_name").cast("string"),
            F.col("card_type").cast("string"),
            F.col("rarity").cast("string"),
            F.col("target_attr").cast("string"),
            F.col("target_weapon").cast("string"),
            F.col("upgrade_attr").cast("string"),
            F.col("effect_value").cast("double"),
            F.col("min_wave").cast("int"),
            F.col("max_wave").cast("int"),
            F.col("build_tags").cast("string"),
        )
    )


def load_weapon_dim(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("header", True)
        .csv(resolve_read_path(path))
        .select(
            F.col("weapon_id").cast("string"),
            F.col("weapon_name").cast("string"),
            F.col("base_damage").cast("double"),
            F.col("base_cooldown").cast("double"),
            F.col("base_attack_count").cast("double"),
            F.col("base_frequency").cast("double"),
            F.col("base_range").cast("double"),
            F.col("damage_growth").cast("double"),
            F.col("cooldown_growth").cast("double"),
            F.col("attack_count_growth").cast("double"),
            F.col("frequency_growth").cast("double"),
            F.col("range_growth").cast("double"),
        )
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


def build_card_strength_report(card_pick: DataFrame, player_match: DataFrame, card_dim: DataFrame) -> DataFrame:
    show_counts = (
        card_pick.filter((F.col("event_type") == "card_choice") & F.col("candidate_card_ids").isNotNull())
        .select(F.explode_outer("candidate_card_ids").alias("card_id"))
        .filter(F.col("card_id").isNotNull())
        .groupBy("card_id")
        .agg(F.count("*").alias("show_count"))
    )

    pick_events = card_pick.filter((F.col("event_type") == "card_pick") & F.col("card_id").isNotNull())
    pick_counts = pick_events.groupBy("card_id").agg(F.count("*").alias("pick_count"))

    player_card = (
        pick_events.select("dt", "match_id", "player_id", "card_id")
        .dropDuplicates(["dt", "match_id", "player_id", "card_id"])
        .join(
            player_match.select(
                "dt",
                "match_id",
                "player_id",
                F.coalesce(F.col("success_50").cast("int"), F.lit(0)).alias("success_50"),
                F.coalesce(F.col("survival_wave").cast("double"), F.lit(0.0)).alias("survival_wave"),
                F.coalesce(F.col("damage_dealt").cast("double"), F.lit(0.0)).alias("damage_dealt"),
                F.coalesce(F.col("kill_count").cast("double"), F.lit(0.0)).alias("kill_count"),
                F.coalesce(F.col("heal_done").cast("double"), F.lit(0.0)).alias("heal_done"),
                F.coalesce(F.col("damage_taken").cast("double"), F.lit(0.0)).alias("damage_taken"),
            ),
            ["dt", "match_id", "player_id"],
            "inner",
        )
    )

    baseline = player_match.agg(
        F.avg(F.coalesce(F.col("success_50").cast("double"), F.lit(0.0))).alias("baseline_success_50_rate"),
        F.avg(F.coalesce(F.col("survival_wave").cast("double"), F.lit(0.0))).alias("baseline_survival_wave"),
        F.avg(F.coalesce(F.col("damage_dealt").cast("double"), F.lit(0.0))).alias("baseline_damage_dealt"),
        F.avg(F.coalesce(F.col("kill_count").cast("double"), F.lit(0.0))).alias("baseline_kill_count"),
        F.avg(F.coalesce(F.col("heal_done").cast("double"), F.lit(0.0))).alias("baseline_heal_done"),
        F.avg(F.coalesce(F.col("damage_taken").cast("double"), F.lit(0.0))).alias("baseline_damage_taken"),
    )

    card_perf = (
        player_card.groupBy("card_id")
        .agg(
            F.count("*").alias("pick_player_count"),
            F.avg("success_50").alias("success_50_rate_with_card"),
            F.avg("survival_wave").alias("avg_survival_wave_with_card"),
            F.avg("damage_dealt").alias("avg_damage_dealt_with_card"),
            F.avg("kill_count").alias("avg_kill_count_with_card"),
            F.avg("heal_done").alias("avg_heal_done_with_card"),
            F.avg("damage_taken").alias("avg_damage_taken_with_card"),
        )
        .crossJoin(baseline)
        .withColumn("success_50_lift", F.col("success_50_rate_with_card") - F.col("baseline_success_50_rate"))
        .withColumn("wave_lift", F.col("avg_survival_wave_with_card") - F.col("baseline_survival_wave"))
        .withColumn("damage_lift", F.col("avg_damage_dealt_with_card") - F.col("baseline_damage_dealt"))
        .withColumn("kill_lift", F.col("avg_kill_count_with_card") - F.col("baseline_kill_count"))
        .withColumn("heal_lift", F.col("avg_heal_done_with_card") - F.col("baseline_heal_done"))
        .withColumn("damage_taken_lift", F.col("avg_damage_taken_with_card") - F.col("baseline_damage_taken"))
    )

    joined = (
        card_dim.join(show_counts, ["card_id"], "left")
        .join(pick_counts, ["card_id"], "left")
        .join(card_perf, ["card_id"], "left")
        .fillna(
            {
                "show_count": 0,
                "pick_count": 0,
                "pick_player_count": 0,
                "success_50_rate_with_card": 0.0,
                "avg_survival_wave_with_card": 0.0,
                "avg_damage_dealt_with_card": 0.0,
                "avg_kill_count_with_card": 0.0,
                "avg_heal_done_with_card": 0.0,
                "avg_damage_taken_with_card": 0.0,
                "success_50_lift": 0.0,
                "wave_lift": 0.0,
                "damage_lift": 0.0,
                "kill_lift": 0.0,
                "heal_lift": 0.0,
                "damage_taken_lift": 0.0,
            }
        )
        .withColumn(
            "pick_rate",
            F.when(F.greatest(F.col("show_count"), F.col("pick_count")) <= 0, F.lit(0.0)).otherwise(
                F.col("pick_count") / F.greatest(F.col("show_count"), F.col("pick_count"))
            ),
        )
        .withColumn("effective_show_count", F.greatest(F.col("show_count"), F.col("pick_count")))
        .withColumn("is_overpowered_tag", F.instr(F.coalesce(F.col("build_tags"), F.lit("")), "overpowered") > 0)
    )

    joined = normalize_global(joined, "pick_rate", "norm_pick_rate")
    joined = normalize_global(joined, "success_50_lift", "norm_success_50_lift")
    joined = normalize_global(joined, "wave_lift", "norm_wave_lift")

    report = (
        joined.withColumn(
            "strength_score",
            F.lit(0.35) * F.col("norm_pick_rate")
            + F.lit(0.40) * F.col("norm_success_50_lift")
            + F.lit(0.25) * F.col("norm_wave_lift"),
        )
        .withColumn("dt", F.lit("all"))
        .select(
            "dt",
            "card_id",
            "card_name",
            "card_type",
            "rarity",
            "target_attr",
            "target_weapon",
            "upgrade_attr",
            "effect_value",
            "min_wave",
            "max_wave",
            "show_count",
            "effective_show_count",
            "pick_count",
            F.round("pick_rate", 6).alias("pick_rate"),
            "pick_player_count",
            F.round("success_50_lift", 6).alias("success_50_lift"),
            F.round("wave_lift", 6).alias("wave_lift"),
            F.round("damage_lift", 4).alias("damage_lift"),
            F.round("kill_lift", 4).alias("kill_lift"),
            F.round("heal_lift", 4).alias("heal_lift"),
            F.round("damage_taken_lift", 4).alias("damage_taken_lift"),
            F.round("strength_score", 6).alias("strength_score"),
            "is_overpowered_tag",
            "build_tags",
        )
    )
    return report


def build_weapon_growth_curve_report(card_dim: DataFrame, weapon_dim: DataFrame) -> DataFrame:
    upgrades = card_dim.filter(F.col("card_type") == "weapon_upgrade")

    joined = upgrades.join(weapon_dim, upgrades.target_weapon == weapon_dim.weapon_id, "left")

    base_value = (
        F.when(F.col("upgrade_attr") == "damage", F.col("base_damage"))
        .when(F.col("upgrade_attr") == "cooldown", F.col("base_cooldown"))
        .when(F.col("upgrade_attr") == "attack_count", F.col("base_attack_count"))
        .when(F.col("upgrade_attr") == "frequency", F.col("base_frequency"))
        .when(F.col("upgrade_attr") == "range", F.col("base_range"))
        .otherwise(F.lit(None).cast("double"))
    )

    growth_value = (
        F.when(F.col("upgrade_attr") == "damage", F.col("damage_growth"))
        .when(F.col("upgrade_attr") == "cooldown", F.col("cooldown_growth"))
        .when(F.col("upgrade_attr") == "attack_count", F.col("attack_count_growth"))
        .when(F.col("upgrade_attr") == "frequency", F.col("frequency_growth"))
        .when(F.col("upgrade_attr") == "range", F.col("range_growth"))
        .otherwise(F.lit(None).cast("double"))
    )

    effect = F.coalesce(F.col("effect_value"), F.lit(0.0))
    up_factor = F.pow(F.lit(1.0) + effect, F.lit(1.0))
    up_factor_3 = F.pow(F.lit(1.0) + effect, F.lit(3.0))
    up_factor_5 = F.pow(F.lit(1.0) + effect, F.lit(5.0))

    # Cooldown is better when lower, so we apply inverse progression.
    down_factor = F.pow(F.greatest(F.lit(0.01), F.lit(1.0) - effect), F.lit(1.0))
    down_factor_3 = F.pow(F.greatest(F.lit(0.01), F.lit(1.0) - effect), F.lit(3.0))
    down_factor_5 = F.pow(F.greatest(F.lit(0.01), F.lit(1.0) - effect), F.lit(5.0))

    value_after_1 = F.when(F.col("upgrade_attr") == "cooldown", base_value * down_factor).otherwise(base_value * up_factor)
    value_after_3 = F.when(F.col("upgrade_attr") == "cooldown", base_value * down_factor_3).otherwise(base_value * up_factor_3)
    value_after_5 = F.when(F.col("upgrade_attr") == "cooldown", base_value * down_factor_5).otherwise(base_value * up_factor_5)

    return (
        joined.withColumn("weapon_base_value", base_value)
        .withColumn("weapon_growth_value", growth_value)
        .withColumn("value_after_1_pick", value_after_1)
        .withColumn("value_after_3_pick", value_after_3)
        .withColumn("value_after_5_pick", value_after_5)
        .withColumn("dt", F.lit("all"))
        .select(
            "dt",
            "card_id",
            "card_name",
            "target_weapon",
            "weapon_name",
            "upgrade_attr",
            F.round("effect_value", 6).alias("upgrade_effect_value"),
            F.round("weapon_base_value", 6).alias("weapon_base_value"),
            F.round("weapon_growth_value", 6).alias("weapon_growth_value"),
            F.round("value_after_1_pick", 6).alias("value_after_1_pick"),
            F.round("value_after_3_pick", 6).alias("value_after_3_pick"),
            F.round("value_after_5_pick", 6).alias("value_after_5_pick"),
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
        card_pick = read_dataset(spark, args.dwd_input, "card_pick_detail")
        player_match = read_dataset(spark, args.dws_input, "player_match_summary")
        card_dim = load_card_dim(spark, args.card_config)
        weapon_dim = load_weapon_dim(spark, args.weapon_config)

        strength_report = build_card_strength_report(card_pick, player_match, card_dim)
        growth_curve_report = build_weapon_growth_curve_report(card_dim, weapon_dim)

        write_dataset(strength_report, args.output, "card_strength_report", args.overwrite)
        write_dataset(growth_curve_report, args.output, "card_weapon_growth_curve_report", args.overwrite)

        if args.show_counts:
            print(f"card_strength_report: {strength_report.count()}")
            print(f"card_weapon_growth_curve_report: {growth_curve_report.count()}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
