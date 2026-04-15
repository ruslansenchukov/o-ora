#!/usr/bin/env python3
import argparse
import math
import os
import statistics
import sys
import time
from typing import Dict, Tuple

from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark oracle11 connector (baseline vs tuned) with repeated count() runs."
    )
    parser.add_argument("--url", required=True, help="JDBC URL")
    parser.add_argument("--user", required=True, help="Oracle username")
    parser.add_argument("--password", help="Oracle password (or ORA11_PASSWORD env var)")

    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--dbtable", help="Table name, e.g. HR.EMPLOYEES")
    source_group.add_argument("--query", help="SQL query")

    parser.add_argument("--warmup", type=int, default=1, help="Warmup iterations per scenario")
    parser.add_argument("--runs", type=int, default=3, help="Measured iterations per scenario")
    parser.add_argument("--master", default="local[2]", help='Spark master (default: "local[2]")')
    parser.add_argument("--app-name", default="oracle11-benchmark", help="Spark app name")

    parser.add_argument("--baseline-fetchsize", type=int, default=1000)
    parser.add_argument("--tuned-fetchsize", type=int, default=5000)
    parser.add_argument("--connect-timeout-ms", type=int, default=5000)
    parser.add_argument("--read-timeout-ms", type=int, default=120000)
    parser.add_argument("--query-timeout-sec", type=int, default=300)
    parser.add_argument("--max-in-list-size", type=int, default=1000)
    parser.add_argument("--schema-cache-ttl-sec", type=int, default=300)

    parser.add_argument("--partition-column")
    parser.add_argument("--lower-bound")
    parser.add_argument("--upper-bound")
    parser.add_argument("--num-partitions", type=int)
    parser.add_argument("--auto-partition-bounds", action="store_true")
    parser.add_argument("--auto-partition-min-rows-per-partition", type=int, default=100000)

    return parser.parse_args()


def validate_partition_args(args: argparse.Namespace) -> None:
    if args.auto_partition_bounds:
        if args.partition_column is None or args.num_partitions is None:
            raise ValueError("--auto-partition-bounds requires --partition-column and --num-partitions")
        if args.lower_bound is not None or args.upper_bound is not None:
            raise ValueError("--lower-bound/--upper-bound must not be used with --auto-partition-bounds")
        return

    values = [args.partition_column, args.lower_bound, args.upper_bound, args.num_partitions]
    present = [value is not None for value in values]
    if any(present) and not all(present):
        raise ValueError(
            "manual partitioning requires --partition-column --lower-bound --upper-bound --num-partitions"
        )


def build_common_options(args: argparse.Namespace, password: str) -> Dict[str, str]:
    options = {
        "url": args.url,
        "user": args.user,
        "password": password,
    }
    if args.dbtable:
        options["dbtable"] = args.dbtable
    else:
        options["query"] = args.query

    if args.auto_partition_bounds:
        options["partitionColumn"] = args.partition_column
        options["numPartitions"] = str(args.num_partitions)
        options["autoPartitionBounds"] = "true"
        options["autoPartitionMinRowsPerPartition"] = str(args.auto_partition_min_rows_per_partition)
    elif args.partition_column is not None:
        options["partitionColumn"] = args.partition_column
        options["lowerBound"] = args.lower_bound
        options["upperBound"] = args.upper_bound
        options["numPartitions"] = str(args.num_partitions)

    return options


def run_case(
    spark: SparkSession,
    common_options: Dict[str, str],
    scenario_options: Dict[str, str],
    warmup: int,
    runs: int,
) -> Tuple[int, list]:
    durations = []
    row_count = -1

    all_options = dict(common_options)
    all_options.update(scenario_options)

    for _ in range(warmup):
        reader = spark.read.format("oracle11")
        for key, value in all_options.items():
            reader = reader.option(key, value)
        _ = reader.load().count()

    for _ in range(runs):
        reader = spark.read.format("oracle11")
        for key, value in all_options.items():
            reader = reader.option(key, value)
        start = time.perf_counter()
        row_count = reader.load().count()
        elapsed = time.perf_counter() - start
        durations.append(elapsed)

    return row_count, durations


def summarize(name: str, row_count: int, durations: list) -> Dict[str, float]:
    avg_sec = statistics.mean(durations)
    p50_sec = statistics.median(durations)
    ordered = sorted(durations)
    p90_index = max(0, math.ceil(0.9 * len(ordered)) - 1)
    p90_sec = ordered[p90_index]
    rows_per_sec = row_count / avg_sec if avg_sec > 0 else 0.0

    print(f"\n{name}")
    print(f"  rows: {row_count}")
    print(f"  runs: {len(durations)}")
    print(f"  avg_sec: {avg_sec:.4f}")
    print(f"  p50_sec: {p50_sec:.4f}")
    print(f"  p90_sec: {p90_sec:.4f}")
    print(f"  rows_per_sec: {rows_per_sec:.2f}")

    return {"avg_sec": avg_sec, "rows_per_sec": rows_per_sec}


def main() -> int:
    args = parse_args()
    password = args.password or os.getenv("ORA11_PASSWORD")
    if not password:
        print("error: provide --password or ORA11_PASSWORD", file=sys.stderr)
        return 2

    try:
        validate_partition_args(args)
    except ValueError as err:
        print(f"error: {err}", file=sys.stderr)
        return 2

    spark = SparkSession.builder.appName(args.app_name).master(args.master).getOrCreate()

    try:
        common_options = build_common_options(args, password)
        baseline_options = {
            "fetchsize": str(args.baseline_fetchsize),
        }
        tuned_options = {
            "fetchsize": str(args.tuned_fetchsize),
            "connectTimeoutMs": str(args.connect_timeout_ms),
            "readTimeoutMs": str(args.read_timeout_ms),
            "queryTimeoutSec": str(args.query_timeout_sec),
            "maxInListSize": str(args.max_in_list_size),
            "schemaCacheTtlSec": str(args.schema_cache_ttl_sec),
        }

        base_rows, base_durations = run_case(
            spark, common_options, baseline_options, warmup=args.warmup, runs=args.runs
        )
        tuned_rows, tuned_durations = run_case(
            spark, common_options, tuned_options, warmup=args.warmup, runs=args.runs
        )

        baseline = summarize("Baseline", base_rows, base_durations)
        tuned = summarize("Tuned", tuned_rows, tuned_durations)

        delta_pct = ((baseline["avg_sec"] - tuned["avg_sec"]) / baseline["avg_sec"]) * 100.0
        rps_delta_pct = (
            ((tuned["rows_per_sec"] - baseline["rows_per_sec"]) / baseline["rows_per_sec"]) * 100.0
            if baseline["rows_per_sec"] > 0
            else 0.0
        )

        print("\nDelta")
        print(f"  avg_sec improvement: {delta_pct:.2f}%")
        print(f"  rows_per_sec improvement: {rps_delta_pct:.2f}%")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
