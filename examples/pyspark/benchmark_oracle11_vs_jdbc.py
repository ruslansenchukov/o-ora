#!/usr/bin/env python3
import argparse
import math
import os
import statistics
import sys
import time
from typing import Callable, Dict, List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

LOCALE_FIX_JAVA_OPTS = "-Duser.language=en -Duser.country=US"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark comparison: custom oracle11 datasource vs Spark built-in jdbc datasource."
    )
    parser.add_argument("--url", required=True, help="JDBC URL, e.g. jdbc:oracle:thin:@//host:1521/SERVICE")
    parser.add_argument("--user", required=True, help="Oracle username")
    parser.add_argument("--password", help="Oracle password (or ORA11_PASSWORD env var)")

    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--dbtable", help="Table name, e.g. APP_USER.ORDERS_V2")
    source_group.add_argument("--query", help="SQL query")

    parser.add_argument("--master", default="local[2]", help='Spark master (default: "local[2]")')
    parser.add_argument("--app-name", default="oracle11-vs-jdbc-benchmark", help="Spark app name")
    parser.add_argument("--warmup", type=int, default=1, help="Warmup runs per datasource")
    parser.add_argument("--runs", type=int, default=5, help="Measured runs per datasource")
    parser.add_argument(
        "--run-order",
        choices=["alternate", "oracle11-first", "jdbc-first"],
        default="alternate",
        help="Execution order of measured runs (default: alternate)",
    )

    parser.add_argument("--oracle11-fetchsize", type=int, default=5000, help="Fetch size for oracle11")
    parser.add_argument("--jdbc-fetchsize", type=int, default=5000, help="Fetch size for Spark JDBC")

    parser.add_argument("--connect-timeout-ms", type=int, default=5000, help="oracle11 connect timeout")
    parser.add_argument("--read-timeout-ms", type=int, default=120000, help="oracle11 read timeout")
    parser.add_argument("--query-timeout-sec", type=int, default=300, help="oracle11 query timeout")
    parser.add_argument("--max-in-list-size", type=int, default=1000, help="oracle11 IN chunk size")
    parser.add_argument("--schema-cache-ttl-sec", type=int, default=300, help="oracle11 schema cache ttl")

    parser.add_argument("--partition-column", help="Manual partition column for both readers")
    parser.add_argument("--lower-bound", help="Manual lower bound for both readers")
    parser.add_argument("--upper-bound", help="Manual upper bound for both readers")
    parser.add_argument("--num-partitions", type=int, help="Manual partition count for both readers")

    parser.add_argument(
        "--auto-partition-bounds",
        action="store_true",
        help="Enable auto bounds for oracle11 only (Spark JDBC does not support this mode)",
    )
    parser.add_argument(
        "--auto-partition-min-rows-per-partition",
        type=int,
        default=100000,
        help="oracle11 auto bounds row threshold",
    )
    parser.add_argument(
        "--select-cols",
        help="Comma-separated projection applied after read, e.g. ID,STATUS,CREATED_AT",
    )
    parser.add_argument(
        "--filter-sql",
        help='Spark SQL filter string applied after read, e.g. "ID BETWEEN 100 AND 1000"',
    )
    parser.add_argument(
        "--in-column",
        help="Column name for generated IN filter stress test",
    )
    parser.add_argument(
        "--in-start",
        type=int,
        default=1,
        help="Start value for generated IN filter",
    )
    parser.add_argument(
        "--in-count",
        type=int,
        default=0,
        help="Number of integer values in generated IN list (0 = disabled)",
    )
    parser.add_argument(
        "--disable-locale-fix",
        action="store_true",
        help="Disable automatic en_US JVM locale fix for Oracle JDBC (default: enabled)",
    )
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


def apply_source_options(reader, args: argparse.Namespace):
    if args.dbtable:
        return reader.option("dbtable", args.dbtable)
    return reader.option("query", args.query)


def apply_manual_partition_options(reader, args: argparse.Namespace):
    if args.partition_column is None:
        return reader
    return (
        reader.option("partitionColumn", args.partition_column)
        .option("lowerBound", args.lower_bound)
        .option("upperBound", args.upper_bound)
        .option("numPartitions", str(args.num_partitions))
    )


def oracle11_dataframe(spark: SparkSession, args: argparse.Namespace, password: str) -> DataFrame:
    reader = (
        spark.read.format("oracle11")
        .option("url", args.url)
        .option("user", args.user)
        .option("password", password)
        .option("fetchsize", str(args.oracle11_fetchsize))
        .option("connectTimeoutMs", str(args.connect_timeout_ms))
        .option("readTimeoutMs", str(args.read_timeout_ms))
        .option("queryTimeoutSec", str(args.query_timeout_sec))
        .option("maxInListSize", str(args.max_in_list_size))
        .option("schemaCacheTtlSec", str(args.schema_cache_ttl_sec))
    )
    reader = apply_source_options(reader, args)

    if args.auto_partition_bounds:
        reader = (
            reader.option("partitionColumn", args.partition_column)
            .option("numPartitions", str(args.num_partitions))
            .option("autoPartitionBounds", "true")
            .option("autoPartitionMinRowsPerPartition", str(args.auto_partition_min_rows_per_partition))
        )
    else:
        reader = apply_manual_partition_options(reader, args)

    return reader.load()


def jdbc_dataframe(spark: SparkSession, args: argparse.Namespace, password: str) -> DataFrame:
    reader = (
        spark.read.format("jdbc")
        .option("url", args.url)
        .option("user", args.user)
        .option("password", password)
        .option("driver", "oracle.jdbc.OracleDriver")
        .option("fetchsize", str(args.jdbc_fetchsize))
    )
    reader = apply_source_options(reader, args)
    # Spark JDBC supports only manual partition bounds; auto bounds is custom oracle11 feature.
    if not args.auto_partition_bounds:
        reader = apply_manual_partition_options(reader, args)
    return reader.load()


def run_sequence(order: str, run_index: int) -> List[str]:
    if order == "oracle11-first":
        return ["oracle11", "jdbc"]
    if order == "jdbc-first":
        return ["jdbc", "oracle11"]
    if run_index % 2 == 0:
        return ["oracle11", "jdbc"]
    return ["jdbc", "oracle11"]


def run_single_count(frame_builder, spark: SparkSession) -> Tuple[int, float]:
    start = time.perf_counter()
    row_count = frame_builder(spark).count()
    elapsed = time.perf_counter() - start
    return row_count, elapsed


def execute_benchmark(
    spark: SparkSession,
    args: argparse.Namespace,
    builders: Dict[str, Callable[[SparkSession], DataFrame]],
) -> Tuple[Dict[str, int], Dict[str, List[float]]]:
    rows = {"oracle11": -1, "jdbc": -1}
    durations = {"oracle11": [], "jdbc": []}

    for i in range(args.warmup):
        for source in run_sequence(args.run_order, i):
            rows[source], _ = run_single_count(builders[source], spark)

    for i in range(args.runs):
        for source in run_sequence(args.run_order, i):
            row_count, elapsed = run_single_count(builders[source], spark)
            rows[source] = row_count
            durations[source].append(elapsed)

    return rows, durations


def summarize(name: str, row_count: int, durations: List[float]) -> Dict[str, float]:
    avg_sec = statistics.mean(durations)
    p50_sec = statistics.median(durations)
    ordered = sorted(durations)
    p90_index = max(0, math.ceil(0.9 * len(ordered)) - 1)
    p90_sec = ordered[p90_index]
    min_sec = ordered[0]
    max_sec = ordered[-1]
    stddev_sec = statistics.stdev(durations) if len(durations) > 1 else 0.0
    rows_per_sec = row_count / avg_sec if avg_sec > 0 else 0.0

    print(f"\n{name}")
    print(f"  rows: {row_count}")
    print(f"  runs: {len(durations)}")
    print(f"  avg_sec: {avg_sec:.4f}")
    print(f"  p50_sec: {p50_sec:.4f}")
    print(f"  p90_sec: {p90_sec:.4f}")
    print(f"  min_sec: {min_sec:.4f}")
    print(f"  max_sec: {max_sec:.4f}")
    print(f"  stddev_sec: {stddev_sec:.4f}")
    print(f"  rows_per_sec: {rows_per_sec:.2f}")
    return {"avg_sec": avg_sec, "rows_per_sec": rows_per_sec}


def apply_workload(df: DataFrame, args: argparse.Namespace) -> DataFrame:
    if args.select_cols:
        columns = [c.strip() for c in args.select_cols.split(",") if c.strip()]
        if columns:
            df = df.select(*columns)

    if args.filter_sql:
        df = df.where(args.filter_sql)

    if args.in_column and args.in_count > 0:
        values = list(range(args.in_start, args.in_start + args.in_count))
        df = df.where(col(args.in_column).isin(values))

    return df


def create_spark_session(args: argparse.Namespace) -> SparkSession:
    builder = SparkSession.builder.appName(args.app_name).master(args.master)
    if not args.disable_locale_fix:
        # Executor-side locale is important for Spark JDBC tasks.
        builder = builder.config("spark.executor.extraJavaOptions", LOCALE_FIX_JAVA_OPTS)
        # Driver option is kept for completeness; driver locale is also forced at runtime below.
        builder = builder.config("spark.driver.extraJavaOptions", LOCALE_FIX_JAVA_OPTS)

    spark = builder.getOrCreate()

    if not args.disable_locale_fix:
        # Ensure the current driver JVM process also uses en_US before first JDBC connection.
        jvm = spark.sparkContext._jvm
        jvm.java.util.Locale.setDefault(jvm.java.util.Locale.US)
        jvm.java.lang.System.setProperty("user.language", "en")
        jvm.java.lang.System.setProperty("user.country", "US")

    return spark


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

    spark = create_spark_session(args)

    try:
        builders = {
            "oracle11": lambda s: apply_workload(oracle11_dataframe(s, args, password), args),
            "jdbc": lambda s: apply_workload(jdbc_dataframe(s, args, password), args),
        }
        rows, timings = execute_benchmark(spark=spark, args=args, builders=builders)
        print(
            f"oracle11: rows={rows['oracle11']}, runs={len(timings['oracle11'])}, run_order={args.run_order}"
        )
        print(
            f"jdbc: rows={rows['jdbc']}, runs={len(timings['jdbc'])}, run_order={args.run_order}"
        )

        oracle_stats = summarize("oracle11", rows["oracle11"], timings["oracle11"])
        jdbc_stats = summarize("jdbc", rows["jdbc"], timings["jdbc"])

        print("\nDelta (oracle11 vs jdbc)")
        avg_improvement = ((jdbc_stats["avg_sec"] - oracle_stats["avg_sec"]) / jdbc_stats["avg_sec"]) * 100.0
        rps_improvement = (
            ((oracle_stats["rows_per_sec"] - jdbc_stats["rows_per_sec"]) / jdbc_stats["rows_per_sec"]) * 100.0
            if jdbc_stats["rows_per_sec"] > 0
            else 0.0
        )
        print(f"  avg_sec improvement: {avg_improvement:.2f}%")
        print(f"  rows_per_sec improvement: {rps_improvement:.2f}%")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
