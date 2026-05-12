"""Export HDFS ADS datasets to local files for Streamlit."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export ADS parquet datasets to local CSV/Parquet files.")
    parser.add_argument("--input", required=True, help="ADS root path, local or hdfs:/// path.")
    parser.add_argument("--output", required=True, help="Local output directory for Streamlit files.")
    parser.add_argument(
        "--datasets",
        default="hero_balance_report,map_grid_heatmap_report,card_strength_report,card_weapon_growth_curve_report,build_combination_report",
        help="Comma-separated ADS dataset names to export.",
    )
    parser.add_argument(
        "--formats",
        default="parquet,csv",
        help="Comma-separated file formats to export. Supports: parquet,csv",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing output files.")
    parser.add_argument("--app-name", default="pve-balance-export-ads", help="Spark application name.")
    return parser.parse_args()


def ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def parse_csv_list(raw: str) -> list[str]:
    return [x.strip() for x in raw.split(",") if x.strip()]


def main() -> None:
    args = parse_args()

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(args.app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        datasets = parse_csv_list(args.datasets)
        formats = set(parse_csv_list(args.formats))
        output_dir = Path(args.output)
        ensure_output_dir(output_dir)

        for dataset in datasets:
            input_path = args.input.rstrip("/") + f"/{dataset}"
            df = spark.read.parquet(input_path)
            rows = [row.asDict(recursive=True) for row in df.collect()]
            pdf = pd.DataFrame(rows, columns=df.columns)

            if "parquet" in formats:
                parquet_file = output_dir / f"{dataset}.parquet"
                if parquet_file.exists() and not args.overwrite:
                    raise FileExistsError(f"File already exists: {parquet_file}")
                pdf.to_parquet(parquet_file, index=False)

            if "csv" in formats:
                csv_file = output_dir / f"{dataset}.csv"
                if csv_file.exists() and not args.overwrite:
                    raise FileExistsError(f"File already exists: {csv_file}")
                pdf.to_csv(csv_file, index=False)

            print(f"exported {dataset}: {len(pdf)} rows")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
