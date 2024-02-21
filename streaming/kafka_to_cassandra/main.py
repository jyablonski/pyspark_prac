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

spark = (
    SparkSession.builder.appName("SparkStructuredStreaming")
    .config("spark.cassandra.connection.host", "cassandra")
    .config("spark.cassandra.connection.port", "9042")
    .config("spark.cassandra.auth.username", "cassandra")
    .config("spark.cassandra.auth.password", "cassandra")
    .config("spark.driver.host", "localhost")
    .getOrCreate()
)


df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "jacobs-topic")
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


def writeToCassandra(writeDF, _):
    writeDF.write.format("org.apache.spark.sql.cassandra").mode("append").options(
        table="jacobs-topic", keyspace="jyablonski"
    ).save()


df1.writeStream.foreachBatch(writeToCassandra).outputMode(
    "update"
).start().awaitTermination()
df1.show()
