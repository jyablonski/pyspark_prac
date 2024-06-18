import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    IntegerType,
    FloatType,
    StringType,
    StructType,
    StructField,
    TimestampType,
)

from pyspark.sql.functions import current_timestamp, from_json, col

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(asctime)s %(message)s",
    datefmt="%Y-%m-%d %I:%M:%S %p",
    handlers=[logging.StreamHandler()],
)

# this helps avoid some unnecessary log messages from java pop up
logging.getLogger("py4j").setLevel(logging.ERROR)

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
spark_packages = [
    "com.datastax.spark/spark-cassandra-connector_2.12/3.4.1",
    "org.apache.spark:spark-sql-kafka-0-10_2.12/3.4.1",
]

# can just set these with environment variables to keep secrets out of code,
# and to change secrets depending on environment
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

# spark readstream is a method part of the structured streaming api
# allows you to read data from kafka, file streams, sockets etc
# it returns a dataframe of data representing the streaming data
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", TOPIC_NAME)
    .option("delimeter", ",")
    .option("startingOffsets", "earliest")  # `latest` / `earliest` are options here
    .load()
)

# the schemas we initially get it from kafka
# root
#  |-- key: binary (nullable = true)
#  |-- value: binary (nullable = true)
#  |-- topic: string (nullable = true)
#  |-- partition: integer (nullable = true)
#  |-- offset: long (nullable = true)
#  |-- timestamp: timestamp (nullable = true)
#  |-- timestampType: integer (nullable = true)
df.printSchema()

# kafka stores the payload as a binary blob by default in the value column
# so you need to parse this into a readable format like a string, and then unnest it
# into your target schema you want
df1 = (
    df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col=col("value"), schema=jacobs_topic_schema).alias("data"))
    .select("data.*")
)
df1.printSchema()


def writeToCassandra(df: DataFrame, _) -> None:
    logging.info("Writing Batch to Cassandra")

    df = df.withColumn("spark_ts", current_timestamp())
    df.write.format("org.apache.spark.sql.cassandra").mode("append").options(
        table="jacobs_topic", keyspace="jyablonski"
    ).save()

    return None


# for each batch is used to apply some tranformation functino on the batch before writing it
cassandra_stream = (
    df1.writeStream.foreachBatch(writeToCassandra)
    .outputMode("update")
    .trigger(processingTime="10 seconds")
    .start()
)

print(cassandra_stream.status)
cassandra_stream.awaitTermination()
