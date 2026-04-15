# spark-oracle11

`spark-oracle11` is a Spark DataSource V2 connector for Oracle via JDBC Thin only.

It targets Spark `3.5.x`, Scala `2.12`, Java `11+`, and does not require Oracle Instant Client, OCI, `tnsnames.ora`, SQL*Plus, or native libraries.

## Build

```bash
sbt clean assembly
```

Artifact:

```text
target/scala-2.12/spark-oracle11-0.1.2-SNAPSHOT-assembly.jar
```

## Usage

### Scala

```scala
val df = spark.read
  .format("oracle11")
  .option("url", "jdbc:oracle:thin:@//host:1521/XE")
  .option("dbtable", "HR.EMPLOYEES")
  .option("user", "hr")
  .option("password", "hr")
  .load()
```

### PySpark

```python
df = (
    spark.read.format("oracle11")
    .option("url", "jdbc:oracle:thin:@//host:1521/XE")
    .option("dbtable", "HR.EMPLOYEES")
    .option("user", "hr")
    .option("password", "hr")
    .load()
)
```

### spark-submit (PySpark example)

```bash
spark-submit \
  --master "local[2]" \
  --jars target/scala-2.12/spark-oracle11-0.1.2-SNAPSHOT-assembly.jar \
  examples/pyspark/read_oracle11_real.py \
  --url "jdbc:oracle:thin:@//host:1521/SERVICE" \
  --dbtable "SCHEMA.TABLE_NAME" \
  --user "YOUR_USER" \
  --password "YOUR_PASSWORD" \
  --count \
  --show 20 \
  --explain
```

## Options

Required:

- `url`
- `user`
- `password`
- exactly one of `dbtable` / `query`

Core optional:

- `fetchsize` (default `1000`)
- `connectTimeoutMs` (no default, disabled if absent)
- `readTimeoutMs` (no default, disabled if absent)
- `queryTimeoutSec` (no default, disabled if absent)
- `maxInListSize` (default `1000`)
- `schemaCacheTtlSec` (default `300`, disable cache with `<= 0`)

Partitioning options:

- `partitionColumn`
- `numPartitions`
- `lowerBound`
- `upperBound`
- `autoPartitionBounds` (default `false`)
- `autoPartitionMinRowsPerPartition` (default `100000`)

Contracts:

- `dbtable` XOR `query`
- manual partitioning requires full set: `partitionColumn + lowerBound + upperBound + numPartitions`
- auto bounds requires: `autoPartitionBounds=true + partitionColumn + numPartitions`
- with auto bounds, `lowerBound/upperBound` must be absent
- for query mode + partitioning, `partitionColumn` must exist in query output

## Oracle type mapping

- `VARCHAR2`, `VARCHAR`, `CHAR`, `NCHAR`, `NVARCHAR2` -> `StringType`
- `NUMBER(p,s)`:
- `s = 0`: `IntegerType` (`p 1..9`), `LongType` (`p 10..18`), `DecimalType(p,0)` (`p 19..38`), else `DecimalType(38,0)`
- `s > 0`: `DecimalType(p,s)` when valid, else `DoubleType`
- `s < 0`: integer-scale `DecimalType` when valid, else `DoubleType`
- `FLOAT` -> `DoubleType`
- `DATE`, `TIMESTAMP` -> `TimestampType`
- `CLOB`/`NCLOB` -> `StringType`
- `BLOB`, `RAW`, `LONG RAW` -> `BinaryType`

## Pushdown

Column pruning:

- supported

Filter pushdown:

- `EqualTo`
- `GreaterThan`
- `GreaterThanOrEqual`
- `LessThan`
- `LessThanOrEqual`
- `In`
- `IsNull`
- `IsNotNull`
- `And`
- `Or`

`In` enhancement:

- large `IN` is chunked into OR-groups based on `maxInListSize` to avoid Oracle `ORA-01795`

Limit pushdown:

- supported via `ROWNUM <= ?`
- for multi-partition reads it is marked as partially pushed so Spark still applies the global limit safely

Safety:

- SQL uses placeholders and typed bind parameters
- unsupported filters are returned as unhandled to Spark

## Partitioning

Manual bounds:

- same semantics as Spark JDBC: bounds define stride, not hard clipping
- generated predicates are gap-free:
- first: `(col < b1 OR col IS NULL)`
- middle: `(col >= bi AND col < b{i+1})`
- last: `(col >= b{n-1})`

Auto bounds:

- when enabled, connector runs `MIN/MAX/COUNT` over partition column (with already pushed predicates)
- effective partition count is capped by both `numPartitions` and `autoPartitionMinRowsPerPartition`
- if stats query fails, connector logs warning and falls back to single partition (no job failure)

Temporal binding:

- Date partitions bind `java.sql.Date`
- Timestamp partitions bind `java.sql.Timestamp`

## Schema inference cache

- Per-JVM in-memory TTL cache keyed by `url + user + relation signature`
- controlled by `schemaCacheTtlSec`
- failed inference attempts are not cached

## Tests

Unit tests:

```bash
sbt test
```

Integration tests:

```bash
sbt -Doracle11.it.enabled=true "IntegrationTest / test"
```

External Oracle target:

```bash
export ORA11_IT_URL='jdbc:oracle:thin:@//host:1521/SERVICE'
export ORA11_IT_USER='user'
export ORA11_IT_PASSWORD='password'
sbt -Doracle11.it.enabled=true "IntegrationTest / test"
```

## Benchmark harness

Script:

- `examples/pyspark/benchmark_oracle11.py`

Example:

```bash
spark-submit \
  --master "local[2]" \
  --jars target/scala-2.12/spark-oracle11-0.1.2-SNAPSHOT-assembly.jar \
  examples/pyspark/benchmark_oracle11.py \
  --url "jdbc:oracle:thin:@//host:1521/SERVICE" \
  --dbtable "SCHEMA.BIG_TABLE" \
  --user "YOUR_USER" \
  --password "YOUR_PASSWORD" \
  --warmup 1 \
  --runs 5 \
  --baseline-fetchsize 1000 \
  --tuned-fetchsize 10000 \
  --partition-column ID \
  --num-partitions 16 \
  --auto-partition-bounds
```

The script prints baseline vs tuned elapsed time and rows/sec deltas.

`oracle11` vs standard Spark JDBC benchmark:

```bash
spark-submit \
  --master "local[2]" \
  --jars target/scala-2.12/spark-oracle11-0.1.2-SNAPSHOT-assembly.jar \
  examples/pyspark/benchmark_oracle11_vs_jdbc.py \
  --url "jdbc:oracle:thin:@//host:1521/SERVICE" \
  --dbtable "SCHEMA.BIG_TABLE" \
  --user "YOUR_USER" \
  --password "YOUR_PASSWORD" \
  --warmup 1 \
  --runs 5 \
  --run-order alternate
```

Fair A/B recommendations:

- keep `fetchsize` and partition settings identical between `oracle11` and `jdbc`
- use the same projection/filter workload on both readers
- use `--run-order alternate` to reduce cache/order bias
- compare `avg`, `p90`, and `stddev` (not avg only)

## Troubleshooting

- `DATA_SOURCE_NOT_FOUND: oracle11`: verify assembly jar path in `--jars`
- `zsh: no matches found: local[2]`: quote master as `"local[2]"`
- `ORA-12705`: connector retries connection once with temporary `en_US` locale override
- `ORA-12705` with standard Spark `format("jdbc")`: use
  `--conf "spark.driver.extraJavaOptions=-Duser.language=en -Duser.country=US"` and
  `--conf "spark.executor.extraJavaOptions=-Duser.language=en -Duser.country=US"`,
  or run `examples/pyspark/benchmark_oracle11_vs_jdbc.py` (locale fix is enabled there by default)
- `Connection refused`: verify host/port/service and firewall/security group
- `NoClassDefFoundError` for Oracle JDBC classes: ensure assembly jar is loaded

## Known limitations

- read-only connector (no write path)
- aggregate pushdown is not implemented
- advanced predicate pushdown (`Not`, `StartsWith`, etc.) is not implemented
- no catalog integration yet


## Benchmark results

```bash
oracle11
  rows: 9981
  runs: 111
  avg_sec: 1.0270
  p50_sec: 1.0158
  p90_sec: 1.1031
  rows_per_sec: 9718.97

jdbc
  rows: 9981
  runs: 111
  avg_sec: 1.7597
  p50_sec: 1.7407
  p90_sec: 1.8690
  rows_per_sec: 5672.09

Delta (oracle11 vs jdbc)
  avg_sec improvement: 41.64%
  rows_per_sec improvement: 71.35%
  ```
