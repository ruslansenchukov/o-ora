#!/usr/bin/env python3
import argparse
import os
import sys

from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read Oracle data via custom Spark DataSource V2 format('oracle11')."
    )
    parser.add_argument("--url", required=True, help="JDBC URL, e.g. jdbc:oracle:thin:@//host:1521/SERVICE")
    parser.add_argument("--user", required=True, help="Oracle username")
    parser.add_argument("--password", help="Oracle password. If omitted, ORA11_PASSWORD env var is used")

    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--dbtable", help="Table name, e.g. HR.EMPLOYEES")
    source_group.add_argument("--query", help="SQL query")

    parser.add_argument("--fetchsize", type=int, default=2000, help="JDBC fetch size (default: 2000)")
    parser.add_argument("--connect-timeout-ms", type=int, help="JDBC connect timeout in ms")
    parser.add_argument("--read-timeout-ms", type=int, help="JDBC read timeout in ms")
    parser.add_argument("--query-timeout-sec", type=int, help="Statement timeout in seconds")
    parser.add_argument("--max-in-list-size", type=int, help="IN chunk size for pushdown")
    parser.add_argument("--schema-cache-ttl-sec", type=int, help="Schema cache TTL in seconds")

    parser.add_argument("--partition-column", help="Partition column")
    parser.add_argument("--lower-bound", help="Lower bound for partitioning")
    parser.add_argument("--upper-bound", help="Upper bound for partitioning")
    parser.add_argument("--num-partitions", type=int, help="Number of partitions")
    parser.add_argument(
        "--auto-partition-bounds",
        action="store_true",
        help="Enable auto MIN/MAX/COUNT partition bound discovery",
    )
    parser.add_argument(
        "--auto-partition-min-rows-per-partition",
        type=int,
        help="Min rows per partition when auto bounds are enabled",
    )

    parser.add_argument("--show", type=int, default=20, help="Rows to show (default: 20)")
    parser.add_argument("--count", action="store_true", help="Also run count()")
    parser.add_argument("--explain", action="store_true", help="Print logical/physical plan")

    parser.add_argument("--app-name", default="oracle11-real-read", help="Spark app name")
    parser.add_argument("--master", default="local[2]", help="Spark master, default local[2]")
    return parser.parse_args()


def validate_partition_args(args: argparse.Namespace) -> None:
    if args.auto_partition_bounds:
        if args.partition_column is None or args.num_partitions is None:
            raise ValueError(
                "--auto-partition-bounds requires --partition-column and --num-partitions"
            )
        if args.lower_bound is not None or args.upper_bound is not None:
            raise ValueError(
                "--lower-bound/--upper-bound must not be used with --auto-partition-bounds"
            )
        return

    values = [args.partition_column, args.lower_bound, args.upper_bound, args.num_partitions]
    present = [v is not None for v in values]
    if any(present) and not all(present):
        raise ValueError(
            "manual partition options must be provided together: "
            "--partition-column --lower-bound --upper-bound --num-partitions"
        )


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

    spark = (
        SparkSession.builder.appName(args.app_name)
        .master(args.master)
        .getOrCreate()
    )

    try:
        reader = (
            spark.read.format("oracle11")
            .option("url", args.url)
            .option("user", args.user)
            .option("password", password)
            .option("fetchsize", str(args.fetchsize))
        )
        if args.connect_timeout_ms is not None:
            reader = reader.option("connectTimeoutMs", str(args.connect_timeout_ms))
        if args.read_timeout_ms is not None:
            reader = reader.option("readTimeoutMs", str(args.read_timeout_ms))
        if args.query_timeout_sec is not None:
            reader = reader.option("queryTimeoutSec", str(args.query_timeout_sec))
        if args.max_in_list_size is not None:
            reader = reader.option("maxInListSize", str(args.max_in_list_size))
        if args.schema_cache_ttl_sec is not None:
            reader = reader.option("schemaCacheTtlSec", str(args.schema_cache_ttl_sec))

        if args.dbtable:
            reader = reader.option("dbtable", args.dbtable)
        else:
            reader = reader.option("query", args.query)

        if args.auto_partition_bounds:
            reader = (
                reader.option("partitionColumn", args.partition_column)
                .option("numPartitions", str(args.num_partitions))
                .option("autoPartitionBounds", "true")
            )
            if args.auto_partition_min_rows_per_partition is not None:
                reader = reader.option(
                    "autoPartitionMinRowsPerPartition",
                    str(args.auto_partition_min_rows_per_partition),
                )
        elif args.partition_column is not None:
            reader = (
                reader.option("partitionColumn", args.partition_column)
                .option("lowerBound", args.lower_bound)
                .option("upperBound", args.upper_bound)
                .option("numPartitions", str(args.num_partitions))
            )

        df = reader.load()

        print("=== Schema ===")
        df.printSchema()

        if args.explain:
            print("=== Explain ===")
            df.explain(True)

        if args.count:
            print("=== Count ===")
            print(df.count())

        print("=== Sample Rows ===")
        df.show(args.show, truncate=False)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
