# spark-oracle11

Spark DataSource V2 connector for Oracle with a **native OCI/JNA backend**.

`format("oracle11")` is now native OCI.  
Alias `format("oracle-native")` points to the same backend.

JDBC is not used inside the native connector path. Spark built-in JDBC is only used as an external benchmark baseline.

## Supported stack

- Spark `3.5.x`
- Scala `2.12`
- Java `11+` (project currently built/tested with Java 17)

## Native OCI requirements

You must have Oracle client native libraries available on the machine where Spark driver/executors run:

- Oracle Instant Client (or full Oracle client)
- `libclntsh` visible via library path

Typical environment setup examples:

- macOS: `DYLD_LIBRARY_PATH=/path/to/instantclient`
- Linux: `LD_LIBRARY_PATH=/path/to/instantclient`

## Build

```bash
sbt clean compile
sbt assembly
```

Assembly artifact:

```text
target/scala-2.12/spark-oracle11-0.1.2-SNAPSHOT-assembly.jar
```

## Connection options (native)

Connector accepts two contracts.

1) URL-based:

- `oracle.url` (or alias `url`)
- `oracle.user` (or alias `user`)
- `oracle.password` (or alias `password`)

`oracle.url` can be:

- Oracle JDBC thin URL (`jdbc:oracle:thin:@//host:1521/SERVICE`)
- Oracle Net descriptor / easy connect tail (`//host:1521/SERVICE`, `(DESCRIPTION=...)`)

2) Host-based:

- `oracle.host`
- `oracle.port`
- exactly one of:
  - `oracle.serviceName`
  - `oracle.sid`
- `oracle.user`
- `oracle.password`

Common read options:

- exactly one of `dbtable` / `query`
- `fetchsize` (default `1000`)
- `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions` (manual range partitioning)

Native v1 limitation:

- `autoPartitionBounds=true` is explicitly not supported

## Spark read examples

### `oracle11` short name (native)

```scala
val df = spark.read
  .format("oracle11")
  .option("url", "jdbc:oracle:thin:@//host:1521/XE")
  .option("user", "app_user")
  .option("password", "app_pass")
  .option("dbtable", "APP_USER.ORDERS_V2")
  .load()
```

### `oracle-native` alias

```scala
val df = spark.read
  .format("oracle-native")
  .option("oracle.host", "62.169.27.185")
  .option("oracle.port", "1523")
  .option("oracle.serviceName", "XE")
  .option("oracle.user", "app_user")
  .option("oracle.password", "app_pass")
  .option("dbtable", "APP_USER.ORDERS_V2")
  .load()
```

## OCI smoke test

Integration smoke suite includes:

- `SELECT 1 AS X FROM DUAL`
- configurable real query test

By default ITs are skipped unless enabled.

Run:

```bash
sbt -Doracle11.it.enabled=true \
    -Doracle.host=62.169.27.185 \
    -Doracle.port=1523 \
    -Doracle.serviceName=XE \
    -Doracle.user=app_user \
    -Doracle.password=app_pass \
    "IntegrationTest / test"
```

Optional query override:

```bash
-Doracle.it.testQuery="SELECT ID, AMOUNT, STATUS, CATEGORY FROM APP_USER.ORDERS_V2 WHERE ROWNUM <= 10"
```

Legacy JDBC-oriented IT suite is disabled by default and requires explicit opt-in:

```bash
-Doracle11.jdbc.it.enabled=true
```

## Benchmark: Native vs Spark JDBC

Script:

- `examples/pyspark/benchmark_oracle11_vs_jdbc.py`

Important:

- Spark JDBC baseline needs external `ojdbc` jar on classpath.
- Core assembly intentionally does not bundle `ojdbc`.

Example:

```bash
spark-submit \
  --master "local[2]" \
  --jars target/scala-2.12/spark-oracle11-0.1.2-SNAPSHOT-assembly.jar,/path/to/ojdbc8.jar \
  examples/pyspark/benchmark_oracle11_vs_jdbc.py \
  --url "jdbc:oracle:thin:@//62.169.27.185:1523/XE" \
  --user "app_user" \
  --password "app_pass" \
  --dbtable "APP_USER.ORDERS_V2" \
  --select-cols "ID,AMOUNT,STATUS,CATEGORY" \
  --fetch-size 5000 \
  --warmup 1 \
  --runs 33 \
  --run-order alternate \
  --jdbc-driver-jar "/path/to/ojdbc8.jar"
```

Output includes:

- per-run CSV-like lines: `engine,run,rows,duration_sec,rows_per_sec`
- summary metrics: `avg/min/max/p50/p95`
- speedup: `native_vs_jdbc`

## Notes and current limitations

- Read-only connector (no write path yet)
- Manual partitioning only in native v1
- `RAW/BLOB` are currently explicit unsupported in native schema mapping path
- Legacy JDBC utilities remain in codebase for shared helper logic and backward compatibility tests, but native connector execution path does not use JDBC transport

