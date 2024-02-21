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

TOPIC_NAME="jacobs-topic"

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
spark_packages = [
    "com.datastax.spark/spark-cassandra-connector_2.12/3.4.1",
    "org.apache.spark:spark-sql-kafka-0-10_2.12/3.4.1",
]


spark = (
    SparkSession.builder.appName("SparkStructuredStreaming")
    .config("spark.cassandra.connection.host", "localhost")
    .config("spark.cassandra.connection.port", "9042")
    .config("spark.cassandra.auth.username", "cassandra")
    .config("spark.cassandra.auth.password", "cassandra")
    .config("spark.driver.host", "localhost")
    .config("spark.jars.packages", ",".join(spark_packages))
    .getOrCreate()
)


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


def writeToCassandra(writeDF, _):
    writeDF.write.format("org.apache.spark.sql.cassandra").mode("append").options(
        table="jacobs_topic", keyspace="jyablonski"
    ).save()


df1.writeStream.foreachBatch(writeToCassandra).outputMode(
    "update"
).start().awaitTermination()
