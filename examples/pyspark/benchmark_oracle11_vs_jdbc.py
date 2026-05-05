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
        description="Benchmark native OCI datasource (oracle11/oracle-native) vs Spark JDBC."
    )

    parser.add_argument("--url", help="Oracle JDBC URL, e.g. jdbc:oracle:thin:@//host:1521/XE")
    parser.add_argument("--ora-url", help="Backward-compatible alias for --url")
    parser.add_argument("--user", help="Oracle username")
    parser.add_argument("--ora-user", help="Backward-compatible alias for --user")
    parser.add_argument("--password", help="Oracle password")
    parser.add_argument("--ora-password", help="Backward-compatible alias for --password")

    parser.add_argument("--dbtable", help="Table name, e.g. APP_USER.ORDERS_V2")
    parser.add_argument("--ora-dbtable", help="Backward-compatible alias for --dbtable")
    parser.add_argument("--query", help="SQL query")
    parser.add_argument("--ora-query", help="Backward-compatible alias for --query")

    parser.add_argument("--master", default="local[2]", help='Spark master (default: "local[2]")')
    parser.add_argument("--app-name", default="oracle-native-vs-jdbc-benchmark", help="Spark app name")
    parser.add_argument("--warmup", type=int, default=1, help="Warmup runs per engine")
    parser.add_argument("--runs", type=int, default=5, help="Measured runs per engine")
    parser.add_argument(
        "--run-order",
        choices=["alternate", "native-first", "jdbc-first", "oracle11-first"],
        default="alternate",
        help="Execution order for measured runs (default: alternate)",
    )

    parser.add_argument("--fetch-size", type=int, default=5000, help="Default fetch size for both engines")
    parser.add_argument("--native-fetchsize", type=int, help="Native fetch size override")
    parser.add_argument("--oracle11-fetchsize", type=int, help="Backward-compatible alias for native fetch size")
    parser.add_argument("--jdbc-fetchsize", type=int, help="JDBC fetch size override")

    parser.add_argument("--partition-column", help="Manual partition column for both readers")
    parser.add_argument("--lower-bound", help="Manual lower bound for both readers")
    parser.add_argument("--upper-bound", help="Manual upper bound for both readers")
    parser.add_argument("--num-partitions", type=int, help="Manual partition count for both readers")

    parser.add_argument("--select-cols", help="Projection, comma-separated, e.g. ID,AMOUNT,STATUS")
    parser.add_argument("--filter-sql", help='SQL filter, e.g. "AMOUNT > 0"')
    parser.add_argument("--in-column", help="Column name for IN workload")
    parser.add_argument("--in-start", type=int, default=1, help="Start value for generated IN workload")
    parser.add_argument("--in-count", type=int, default=0, help="Number of values for generated IN workload")

    parser.add_argument("--jdbc-driver-jar", help="Path to external ojdbc jar for Spark JDBC baseline")
    parser.add_argument(
        "--disable-locale-fix",
        action="store_true",
        help="Disable automatic en_US locale fix for Oracle sessions",
    )
    return parser.parse_args()


def first_non_empty(*values: str) -> str:
    for value in values:
        if value is not None and str(value).strip() != "":
            return str(value).strip()
    return ""


def canonicalize_args(args: argparse.Namespace) -> argparse.Namespace:
    args.url = first_non_empty(args.url, args.ora_url)
    args.user = first_non_empty(args.user, args.ora_user)
    args.password = first_non_empty(args.password, args.ora_password)
    args.dbtable = first_non_empty(args.dbtable, args.ora_dbtable)
    args.query = first_non_empty(args.query, args.ora_query)

    if args.run_order == "oracle11-first":
        args.run_order = "native-first"

    args.native_fetchsize = (
        args.native_fetchsize
        if args.native_fetchsize is not None
        else (args.oracle11_fetchsize if args.oracle11_fetchsize is not None else args.fetch_size)
    )
    args.jdbc_fetchsize = args.jdbc_fetchsize if args.jdbc_fetchsize is not None else args.fetch_size
    return args


def validate_args(args: argparse.Namespace) -> None:
    if not args.url:
        raise ValueError("missing required --url (or --ora-url)")
    if not args.user:
        raise ValueError("missing required --user (or --ora-user)")
    if not args.password:
        raise ValueError("missing required --password (or --ora-password / ORA11_PASSWORD)")

    has_dbtable = bool(args.dbtable)
    has_query = bool(args.query)
    if has_dbtable == has_query:
        raise ValueError("provide exactly one of --dbtable or --query")

    values = [args.partition_column, args.lower_bound, args.upper_bound, args.num_partitions]
    present = [v is not None for v in values]
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


def native_dataframe(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    reader = (
        spark.read.format("oracle-native")
        .option("url", args.url)
        .option("user", args.user)
        .option("password", args.password)
        .option("fetchsize", str(args.native_fetchsize))
    )
    reader = apply_source_options(reader, args)
    reader = apply_manual_partition_options(reader, args)
    return reader.load()


def jdbc_dataframe(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    reader = (
        spark.read.format("jdbc")
        .option("url", args.url)
        .option("user", args.user)
        .option("password", args.password)
        .option("driver", "oracle.jdbc.OracleDriver")
        .option("fetchsize", str(args.jdbc_fetchsize))
    )
    reader = apply_source_options(reader, args)
    reader = apply_manual_partition_options(reader, args)
    return reader.load()


def apply_workload(df: DataFrame, args: argparse.Namespace) -> DataFrame:
    if args.select_cols:
        cols = [c.strip() for c in args.select_cols.split(",") if c.strip()]
        if cols:
            df = df.select(*cols)

    if args.filter_sql:
        df = df.where(args.filter_sql)

    if args.in_column and args.in_count > 0:
        values = list(range(args.in_start, args.in_start + args.in_count))
        df = df.where(col(args.in_column).isin(values))

    return df


def create_spark_session(args: argparse.Namespace) -> SparkSession:
    builder = SparkSession.builder.appName(args.app_name).master(args.master)
    if not args.disable_locale_fix:
        builder = builder.config("spark.driver.extraJavaOptions", LOCALE_FIX_JAVA_OPTS)
        builder = builder.config("spark.executor.extraJavaOptions", LOCALE_FIX_JAVA_OPTS)
    return builder.getOrCreate()


def ensure_jdbc_driver_available(spark: SparkSession, args: argparse.Namespace) -> None:
    jvm = spark.sparkContext._jvm

    def has_driver() -> bool:
        try:
            jvm.java.lang.Class.forName("oracle.jdbc.OracleDriver")
            return True
        except Exception:
            return False

    if has_driver():
        return

    if not args.jdbc_driver_jar:
        raise RuntimeError(
            "Oracle JDBC driver is not on classpath for Spark JDBC baseline.\n"
            "Provide --jdbc-driver-jar /path/to/ojdbc*.jar and include it in spark-submit --jars."
        )

    if not os.path.exists(args.jdbc_driver_jar):
        raise RuntimeError(f"--jdbc-driver-jar path does not exist: {args.jdbc_driver_jar}")

    # In PySpark there is no SparkContext.addJar() API.
    # Driver visibility is controlled by spark-submit --jars.
    # We validated path existence and continue; Spark JDBC load will fail fast with a clear error if classpath is wrong.
    if not has_driver():
        print(
            "warning: oracle.jdbc.OracleDriver is not visible to Class.forName() at preflight time. "
            "Continuing because --jdbc-driver-jar was provided; ensure the same jar is present in spark-submit --jars.",
            file=sys.stderr,
        )


def run_order(order: str, run_index: int) -> List[str]:
    if order == "native-first":
        return ["native", "jdbc"]
    if order == "jdbc-first":
        return ["jdbc", "native"]
    if run_index % 2 == 0:
        return ["native", "jdbc"]
    return ["jdbc", "native"]


def run_single_count(frame_builder: Callable[[SparkSession], DataFrame], spark: SparkSession) -> Tuple[int, float]:
    started = time.monotonic()
    rows = frame_builder(spark).count()
    elapsed = time.monotonic() - started
    return rows, elapsed


def percentile(sorted_values: List[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    index = max(0, math.ceil(p * len(sorted_values)) - 1)
    return sorted_values[index]


def summarize(name: str, rows: int, durations: List[float]) -> Dict[str, float]:
    ordered = sorted(durations)
    avg_sec = statistics.mean(ordered)
    min_sec = ordered[0]
    max_sec = ordered[-1]
    p50_sec = statistics.median(ordered)
    p95_sec = percentile(ordered, 0.95)
    rows_per_sec = rows / avg_sec if avg_sec > 0 else 0.0

    print(f"\n{name}")
    print(f"  rows: {rows}")
    print(f"  runs: {len(ordered)}")
    print(f"  avg_sec: {avg_sec:.4f}")
    print(f"  min_sec: {min_sec:.4f}")
    print(f"  max_sec: {max_sec:.4f}")
    print(f"  p50_sec: {p50_sec:.4f}")
    print(f"  p95_sec: {p95_sec:.4f}")
    print(f"  rows_per_sec: {rows_per_sec:.2f}")

    return {
        "avg_sec": avg_sec,
        "rows_per_sec": rows_per_sec,
    }


def execute(
    spark: SparkSession,
    args: argparse.Namespace,
    builders: Dict[str, Callable[[SparkSession], DataFrame]],
) -> Tuple[Dict[str, int], Dict[str, List[float]]]:
    rows = {"native": -1, "jdbc": -1}
    durations: Dict[str, List[float]] = {"native": [], "jdbc": []}

    for i in range(args.warmup):
        for engine in run_order(args.run_order, i):
            rows[engine], _ = run_single_count(builders[engine], spark)

    print("engine,run,rows,duration_sec,rows_per_sec")
    for i in range(args.runs):
        for engine in run_order(args.run_order, i):
            row_count, elapsed = run_single_count(builders[engine], spark)
            rows[engine] = row_count
            durations[engine].append(elapsed)
            rps = row_count / elapsed if elapsed > 0 else 0.0
            print(f"{engine},{i + 1},{row_count},{elapsed:.6f},{rps:.2f}")

    return rows, durations


def main() -> int:
    args = canonicalize_args(parse_args())

    if not args.password:
        args.password = os.getenv("ORA11_PASSWORD", "")

    if not args.jdbc_driver_jar:
        args.jdbc_driver_jar = os.getenv("JDBC_DRIVER_JAR") or os.getenv("OJDBC_JAR")

    try:
        validate_args(args)
    except ValueError as err:
        print(f"error: {err}", file=sys.stderr)
        return 2

    spark = create_spark_session(args)
    try:
        if not args.disable_locale_fix:
            jvm = spark.sparkContext._jvm
            jvm.java.util.Locale.setDefault(jvm.java.util.Locale.US)
            jvm.java.lang.System.setProperty("user.language", "en")
            jvm.java.lang.System.setProperty("user.country", "US")

        ensure_jdbc_driver_available(spark, args)

        builders = {
            "native": lambda s: apply_workload(native_dataframe(s, args), args),
            "jdbc": lambda s: apply_workload(jdbc_dataframe(s, args), args),
        }

        rows, durations = execute(spark, args, builders)
        print(f"\nnative: rows={rows['native']}, runs={len(durations['native'])}, run_order={args.run_order}")
        print(f"jdbc: rows={rows['jdbc']}, runs={len(durations['jdbc'])}, run_order={args.run_order}")

        native_stats = summarize("native", rows["native"], durations["native"])
        jdbc_stats = summarize("jdbc", rows["jdbc"], durations["jdbc"])

        print("\nDelta (native vs jdbc)")
        avg_improvement = ((jdbc_stats["avg_sec"] - native_stats["avg_sec"]) / jdbc_stats["avg_sec"]) * 100.0
        rps_improvement = (
            ((native_stats["rows_per_sec"] - jdbc_stats["rows_per_sec"]) / jdbc_stats["rows_per_sec"]) * 100.0
            if jdbc_stats["rows_per_sec"] > 0
            else 0.0
        )
        print(f"  avg_sec improvement: {avg_improvement:.2f}%")
        print(f"  rows_per_sec improvement: {rps_improvement:.2f}%")
        print(
            f"  speedup native_vs_jdbc: {(jdbc_stats['avg_sec'] / native_stats['avg_sec']):.3f}x"
            if native_stats["avg_sec"] > 0
            else "  speedup native_vs_jdbc: n/a"
        )
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
