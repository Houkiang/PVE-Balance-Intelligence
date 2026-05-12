"""Mine card and hero combinations with Spark association rules."""

from __future__ import annotations

import argparse
from itertools import combinations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


OUTPUT_COLUMNS = [
    "dt",
    "combination_type",
    "items",
    "antecedent",
    "consequent",
    "support",
    "confidence",
    "lift",
    "avg_final_wave",
    "success_50_rate",
    "sample_count",
    "item_count",
    "items_text",
]


OUTPUT_SCHEMA = T.StructType(
    [
        T.StructField("dt", T.StringType(), False),
        T.StructField("combination_type", T.StringType(), False),
        T.StructField("items", T.ArrayType(T.StringType()), True),
        T.StructField("antecedent", T.ArrayType(T.StringType()), True),
        T.StructField("consequent", T.ArrayType(T.StringType()), True),
        T.StructField("support", T.DoubleType(), True),
        T.StructField("confidence", T.DoubleType(), True),
        T.StructField("lift", T.DoubleType(), True),
        T.StructField("avg_final_wave", T.DoubleType(), True),
        T.StructField("success_50_rate", T.DoubleType(), True),
        T.StructField("sample_count", T.LongType(), True),
        T.StructField("item_count", T.IntegerType(), True),
        T.StructField("items_text", T.StringType(), True),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Mine card and hero combinations from DWS data.")
    parser.add_argument("--input", required=True, help="DWS root path, local or hdfs:/// path.")
    parser.add_argument("--output", required=True, help="ADS output root path, local or hdfs:/// path.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite output dataset.")
    parser.add_argument("--min-support", type=float, default=0.04, help="Minimum itemset support.")
    parser.add_argument("--min-confidence", type=float, default=0.3, help="Minimum association-rule confidence.")
    parser.add_argument("--max-itemset-size", type=int, default=3, help="Maximum itemset size to mine.")
    parser.add_argument("--max-rules", type=int, default=200, help="Keep top N rules per combination type.")
    parser.add_argument("--app-name", default="pve-balance-ads-combo", help="Spark application name.")
    parser.add_argument("--show-counts", action="store_true", help="Print output row count after writing.")
    return parser.parse_args()


def dataset_path(root: str, name: str) -> str:
    return root.rstrip("/") + f"/{name}"


def read_dataset(spark: SparkSession, root: str, name: str) -> DataFrame:
    return spark.read.parquet(dataset_path(root, name))


def write_dataset(df: DataFrame, root: str, name: str, overwrite: bool) -> None:
    mode = "overwrite" if overwrite else "errorifexists"
    df.write.mode(mode).parquet(dataset_path(root, name))


@F.udf(T.ArrayType(T.ArrayType(T.StringType())))
def make_itemsets(items: list[str] | None, max_size: int) -> list[list[str]]:
    if not items:
        return []
    unique_items = sorted({item for item in items if item})
    result: list[list[str]] = []
    upper = min(max_size, len(unique_items))
    for size in range(1, upper + 1):
        result.extend([list(group) for group in combinations(unique_items, size)])
    return result


@F.udf(
    T.ArrayType(
        T.StructType(
            [
                T.StructField("antecedent", T.ArrayType(T.StringType()), False),
                T.StructField("consequent", T.ArrayType(T.StringType()), False),
            ]
        )
    )
)
def make_rules(items: list[str] | None) -> list[dict[str, list[str]]]:
    if not items or len(items) < 2:
        return []
    unique_items = sorted({item for item in items if item})
    rules: list[dict[str, list[str]]] = []
    for size in range(1, len(unique_items)):
        for antecedent in combinations(unique_items, size):
            consequent = [item for item in unique_items if item not in antecedent]
            if consequent:
                rules.append({"antecedent": list(antecedent), "consequent": consequent})
    return rules


def empty_output_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame([], OUTPUT_SCHEMA)


def with_transaction_id(df: DataFrame, transaction_key_cols: list[str]) -> DataFrame:
    return df.withColumn("transaction_id", F.concat_ws("::", *[F.col(col).cast("string") for col in transaction_key_cols]))


def build_exact_hero_team_sets(match_summary: DataFrame) -> DataFrame:
    total_matches = match_summary.count()
    if total_matches == 0:
        return empty_output_df(match_summary.sparkSession)

    return (
        match_summary.filter(F.size(F.col("hero_set")) >= 2)
        .groupBy("hero_set")
        .agg(
            F.count("*").alias("sample_count"),
            F.avg(F.coalesce(F.col("final_wave").cast("double"), F.lit(0.0))).alias("avg_final_wave"),
            F.avg(F.coalesce(F.col("success_50").cast("double"), F.lit(0.0))).alias("success_50_rate"),
        )
        .withColumn("support", F.col("sample_count") / F.lit(float(total_matches)))
        .withColumn("dt", F.lit("all"))
        .withColumn("combination_type", F.lit("hero_team_set"))
        .withColumn("items", F.sort_array(F.col("hero_set")))
        .withColumn("antecedent", F.lit(None).cast(T.ArrayType(T.StringType())))
        .withColumn("consequent", F.lit(None).cast(T.ArrayType(T.StringType())))
        .withColumn("confidence", F.lit(None).cast("double"))
        .withColumn("lift", F.lit(None).cast("double"))
        .withColumn("item_count", F.size(F.col("items")))
        .withColumn("items_text", F.concat_ws(" + ", F.col("items")))
        .select(OUTPUT_COLUMNS)
    )


def build_association_report(
    transactions: DataFrame,
    items_col: str,
    combination_type: str,
    min_support: float,
    min_confidence: float,
    max_itemset_size: int,
    max_rules: int,
) -> DataFrame:
    spark = transactions.sparkSession
    tx = transactions.filter(F.size(F.col(items_col)) >= 2).cache()
    tx_count = tx.count()
    if tx_count == 0:
        return empty_output_df(spark)

    exploded = (
        tx.withColumn("itemset", F.explode(make_itemsets(F.col(items_col), F.lit(max_itemset_size))))
        .withColumn("itemset", F.sort_array(F.col("itemset")))
    )

    itemset_stats = (
        exploded.groupBy("itemset")
        .agg(
            F.countDistinct("transaction_id").alias("sample_count"),
            F.avg(F.coalesce(F.col("final_wave").cast("double"), F.lit(0.0))).alias("avg_final_wave"),
            F.avg(F.coalesce(F.col("success_50").cast("double"), F.lit(0.0))).alias("success_50_rate"),
        )
        .withColumn("support", F.col("sample_count") / F.lit(float(tx_count)))
        .withColumn("item_count", F.size(F.col("itemset")))
        .cache()
    )

    frequent_itemsets = itemset_stats.filter((F.col("item_count") >= 2) & (F.col("support") >= F.lit(min_support)))
    if frequent_itemsets.limit(1).count() == 0:
        return empty_output_df(spark)

    rules = (
        frequent_itemsets.withColumn("rule", F.explode(make_rules(F.col("itemset"))))
        .select(
            F.col("itemset").alias("items"),
            F.col("rule.antecedent").alias("antecedent"),
            F.col("rule.consequent").alias("consequent"),
            "support",
            "avg_final_wave",
            "success_50_rate",
            "sample_count",
            "item_count",
        )
    )

    antecedent_support = itemset_stats.select(
        F.col("itemset").alias("antecedent"),
        F.col("support").alias("antecedent_support"),
    )
    consequent_support = itemset_stats.select(
        F.col("itemset").alias("consequent"),
        F.col("support").alias("consequent_support"),
    )

    scored = (
        rules.join(antecedent_support, ["antecedent"], "left")
        .join(consequent_support, ["consequent"], "left")
        .withColumn(
            "confidence",
            F.when(F.col("antecedent_support") <= 0.0, F.lit(0.0)).otherwise(
                F.col("support") / F.col("antecedent_support")
            ),
        )
        .withColumn(
            "lift",
            F.when((F.col("antecedent_support") <= 0.0) | (F.col("consequent_support") <= 0.0), F.lit(0.0)).otherwise(
                F.col("support") / (F.col("antecedent_support") * F.col("consequent_support"))
            ),
        )
        .filter(F.col("confidence") >= F.lit(min_confidence))
        .orderBy(F.desc("lift"), F.desc("confidence"), F.desc("support"))
        .limit(max_rules)
    )

    return (
        scored.withColumn("dt", F.lit("all"))
        .withColumn("combination_type", F.lit(combination_type))
        .withColumn("items_text", F.concat_ws(" + ", F.col("items")))
        .select(OUTPUT_COLUMNS)
    )


def build_card_rules(
    player_card_set: DataFrame,
    player_match: DataFrame,
    min_support: float,
    min_confidence: float,
    max_itemset_size: int,
    max_rules: int,
) -> DataFrame:
    tx = (
        player_match.select("dt", "match_id", "player_id", "final_wave", "success_50")
        .join(
            player_card_set.select("dt", "match_id", "player_id", F.col("card_set").alias("items")),
            ["dt", "match_id", "player_id"],
            "inner",
        )
    )
    tx = with_transaction_id(tx, ["dt", "match_id", "player_id"])
    return build_association_report(tx, "items", "card_rule", min_support, min_confidence, max_itemset_size, max_rules)


def build_hero_rules(
    match_summary: DataFrame,
    min_support: float,
    min_confidence: float,
    max_itemset_size: int,
    max_rules: int,
) -> DataFrame:
    tx = match_summary.select(
        "dt",
        "match_id",
        F.lit(None).cast("string").alias("player_id"),
        F.col("final_wave").cast("double").alias("final_wave"),
        F.col("success_50").cast("int").alias("success_50"),
        F.col("hero_set").alias("items"),
    )
    tx = with_transaction_id(tx, ["dt", "match_id"])
    return build_association_report(tx, "items", "hero_rule", min_support, min_confidence, max_itemset_size, max_rules)


def main() -> None:
    args = parse_args()
    spark = (
        SparkSession.builder.appName(args.app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        match_summary = read_dataset(spark, args.input, "match_summary")
        player_match = read_dataset(spark, args.input, "player_match_summary")
        player_card_set = read_dataset(spark, args.input, "player_card_set")

        card_rules = build_card_rules(
            player_card_set,
            player_match,
            args.min_support,
            args.min_confidence,
            args.max_itemset_size,
            args.max_rules,
        )
        hero_rules = build_hero_rules(
            match_summary,
            args.min_support,
            args.min_confidence,
            args.max_itemset_size,
            args.max_rules,
        )
        hero_team_sets = build_exact_hero_team_sets(match_summary)

        report = card_rules.unionByName(hero_rules, allowMissingColumns=True).unionByName(
            hero_team_sets,
            allowMissingColumns=True,
        )

        write_dataset(report, args.output, "build_combination_report", args.overwrite)

        if args.show_counts:
            print(f"build_combination_report: {report.count()}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
