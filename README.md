# spark-oracle11

`spark-oracle11` is a custom Spark DataSource V2 connector for reading Oracle Database data through the JDBC Thin driver only.

It is designed for Spark 3.5.x and Scala 2.12, with no dependency on Oracle Instant Client, OCI, `tnsnames.ora`, SQL*Plus, or any native Oracle libraries.

## Why no Instant Client is required

The connector uses only Oracle JDBC Thin (`ojdbc8`) which is a pure Java driver. All communication goes through JDBC over TCP.

## Supported versions

- Spark: `3.5.x`
- Scala: `2.12.x`
- Java: `11+`
- Oracle DB target: Oracle 11g compatibility mode via JDBC Thin

## Build

```bash
sbt clean assembly
```

Resulting fat jar:

```text
target/scala-2.12/spark-oracle11-0.1.0-SNAPSHOT-assembly.jar
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

### spark-shell

```bash
spark-shell \
  --conf "spark.driver.extraJavaOptions=--add-exports=java.base/sun.nio.ch=ALL-UNNAMED" \
  --jars target/scala-2.12/spark-oracle11-0.1.0-SNAPSHOT-assembly.jar
```

### spark-submit

```bash
spark-submit \
  --class your.main.Class \
  --jars target/scala-2.12/spark-oracle11-0.1.0-SNAPSHOT-assembly.jar \
  your-app.jar
```

### PySpark real DB smoke-test

```bash
spark-submit \
  --master local[2] \
  --jars target/scala-2.12/spark-oracle11-0.1.0-SNAPSHOT-assembly.jar \
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
- exactly one of: `dbtable` or `query`

Optional:

- `fetchsize` (default: `1000`)
- `partitionColumn`
- `lowerBound`
- `upperBound`
- `numPartitions`

Rules:

- `dbtable` XOR `query`.
- Partition options must be provided as a full set.
- If `query` is used with partition options, `partitionColumn` must exist in query output schema.

## Oracle type mapping

- `VARCHAR2`, `CHAR`, `NCHAR`, `NVARCHAR2` -> `StringType`
- `NUMBER(p,s)` -> typed numeric policy:
  - `s = 0`: `IntegerType` / `LongType` / `DecimalType`
  - `s > 0`: `DecimalType` where valid, otherwise `DoubleType`
  - `s < 0`: integer-scale `DecimalType` where valid, otherwise `DoubleType`
- `FLOAT` -> `DoubleType`
- `DATE`, `TIMESTAMP` -> `TimestampType`
- `CLOB` -> `StringType`
- `BLOB`, `RAW` -> `BinaryType`

Nullability is inferred from JDBC metadata.

## Pushdown support (v1)

Column pruning:

- Projection pushdown is supported.

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

Unsupported filters are returned to Spark as unhandled.

Safety:

- Predicates are compiled into SQL with bind placeholders (`?`) and typed parameter binding.
- Literal user values are not concatenated as raw SQL.

## Partitioning

Range partitioning is supported for numeric/date/timestamp partition columns.

Generated predicates are gap-free:

- first: `(col < b1 OR col IS NULL)`
- middle: `(col >= bi AND col < b{i+1})`
- last: `(col >= b{n-1})`

Semantics match Spark JDBC strategy where bounds define stride and do not hard-clip out-of-range rows.

## Tests

### Unit tests

```bash
sbt test
```

### Integration tests (Oracle)

Integration tests are opt-in:

```bash
sbt -Doracle11.it.enabled=true "IntegrationTest / test"
```

By default harness tries Testcontainers Oracle XE (`gvenzl/oracle-xe:21-slim`).
On Colima, this project auto-sets `TESTCONTAINERS_RYUK_DISABLED=true` for `IntegrationTest` forks to avoid known socket-mount issues with Ryuk.

You can point tests to an external Oracle instance instead:

```bash
export ORA11_IT_URL='jdbc:oracle:thin:@//host:1521/SERVICE'
export ORA11_IT_USER='user'
export ORA11_IT_PASSWORD='password'
sbt -Doracle11.it.enabled=true "IntegrationTest / test"
```

## Architecture overview

Main classes:

- `Oracle11DataSource`: DataSource V2 entrypoint (`shortName = oracle11`)
- `Oracle11Table`: table abstraction with read capability
- `Oracle11ScanBuilder`: required columns + filter pushdown planning
- `Oracle11Scan`, `Oracle11Batch`: batch read plan construction
- `Oracle11InputPartition`, `Oracle11PartitionReaderFactory`, `Oracle11PartitionReader`: partition-level execution
- `Oracle11Options`: option parsing and validation
- `Oracle11SchemaInference`, `Oracle11TypeMapper`: schema and type handling
- `Oracle11FilterCompiler`: safe filter-to-SQL translation
- `Oracle11PartitionPlanner`: range partition generation
- `Oracle11QueryBuilder`, `Oracle11JdbcUtils`: SQL assembly and JDBC utilities

## Known limitations (v1)

- Read-only connector (no write path).
- Limit pushdown is not enabled by default (extension point exists).
- Filter coverage is limited to basic predicates listed above.
- `dbtable`/`query` are treated as trusted SQL fragments (identifiers within generated predicates/projections are quoted safely).
- Integration tests use Oracle XE container for convenience, not strict Oracle 11g binaries.

## Troubleshooting

- `NoClassDefFoundError` for Oracle classes:
  ensure assembly jar is on Spark classpath.
- `IllegalAccessError` on Java 17/21 (`sun.nio.ch.DirectBuffer`):
  Spark needs JPMS flags (`--add-exports`/`--add-opens`). For tests this project already sets them in `build.sbt`.
- Authentication or network errors:
  verify JDBC URL, user/password, listener/service name.
- Partitioning errors:
  check `partitionColumn` exists in inferred schema and bounds are valid for its type.
- Filters not pushed down:
  unsupported predicates are expected to be evaluated by Spark.

## Suggested next improvements

- Implement `SupportsPushDownLimit` and top-N pushdown.
- Add broader predicate compiler support (`Not`, `StartsWith`, etc.).
- Add aggregate pushdown.
- Add write path.
- Add catalog integration.
