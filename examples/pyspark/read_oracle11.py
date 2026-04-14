from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("read-oracle11").getOrCreate()

df = (
    spark.read.format("oracle11")
    .option("url", "jdbc:oracle:thin:@//host:1521/XE")
    .option("dbtable", "HR.EMPLOYEES")
    .option("user", "hr")
    .option("password", "hr")
    .option("fetchsize", "2000")
    .load()
)

df.show(20, truncate=False)
spark.stop()
