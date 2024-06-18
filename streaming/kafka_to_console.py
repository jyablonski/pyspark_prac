from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    FloatType,
    StringType,
    StructType,
    StructField,
    TimestampType,
)
from pyspark.sql.functions import from_json, col

TOPIC_NAME = "jacobs-topic"


jacobs_topic_schema = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("email", StringType(), False),
        StructField("price", FloatType(), False),
        StructField("dividend", IntegerType(), False),
        StructField("scrape_ts", TimestampType(), False),
    ]
)

# https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10_2.12/3.4.1
spark_packages = ["org.apache.spark:spark-sql-kafka-0-10_2.12/3.4.1"]

spark = (
    SparkSession.builder.appName("ReadFromPostgreSQL")
    .config("spark.driver.host", "localhost")
    .config("spark.jars.packages", ",".join(spark_packages))
    .getOrCreate()
)

print("Spark Version:", spark.version)
print("Scala Version:", spark._jvm.scala.util.Properties.versionString())


df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", TOPIC_NAME)
    .option("delimeter", ",")
    .option("startingOffsets", "earliest")
    .load()
)

df.printSchema()

df1 = (
    df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), jacobs_topic_schema).alias("data"))
    .select("data.*")
)
df1.printSchema()

# write strea

console_stream = (
    df1.writeStream.outputMode("update")
    .format("console")
    .option("truncate", False)
    .start()
)

console_stream.awaitTermination()
