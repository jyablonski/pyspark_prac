# https://medium.com/@suffyan.asad1/handling-data-skew-in-apache-spark-techniques-tips-and-tricks-to-improve-performance-e2934b00b021
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark import SparkConf
import pyspark.sql.functions as F


def prepare_trips_data(spark: SparkSession) -> DataFrame:
    pu_loc_to_change = [
        236,
        132,
        161,
        186,
        142,
        141,
        48,
        239,
        170,
        162,
        230,
        163,
        79,
        234,
        263,
        140,
        238,
        107,
        68,
        138,
        229,
        249,
        237,
        164,
        90,
        43,
        100,
        246,
        231,
        262,
        113,
        233,
        143,
        137,
        114,
        264,
        148,
        151,
    ]

    df = spark.read.parquet("data/trips/*.parquet").withColumn(
        "PULocationID",
        F.when(F.col("PULocationID").isin(pu_loc_to_change), F.lit(237)).otherwise(
            F.col("PULocationID")
        ),
    )
    return df


def create_spark_session_with_aqe_disabled() -> SparkSession:
    conf = (
        SparkConf()
        .set("spark.driver.memory", "4G")
        .set("spark.sql.autoBroadcastJoinThreshold", "-1")
        .set("spark.sql.shuffle.partitions", "201")
        .set("spark.sql.adaptive.enabled", "false")
    )

    spark_session = (
        SparkSession.builder.master("local[8]")
        .config(conf=conf)
        .appName("Read from JDBC tutorial")
        .getOrCreate()
    )

    return spark_session


def create_spark_session_with_aqe_skew_join_enabled() -> SparkSession:
    conf = (
        SparkConf()
        .set("spark.driver.memory", "4G")
        .set("spark.sql.autoBroadcastJoinThreshold", "-1")
        .set("spark.sql.shuffle.partitions", "201")
        .set("spark.sql.adaptive.enabled", "true")
        .set("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .set("spark.sql.adaptive.skewJoin.enabled", "true")
        .set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "3")
        .set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "12K")
    )

    spark_session = (
        SparkSession.builder.master("local[8]")
        .config(conf=conf)
        .appName("Read from JDBC tutorial")
        .getOrCreate()
    )

    return spark_session


def create_spark_session_with_aqe_enabled() -> SparkSession:
    conf = (
        SparkConf()
        .set("spark.driver.memory", "4G")
        .set("spark.sql.autoBroadcastJoinThreshold", "-1")  # disables broadcast joins
        .set("spark.sql.shuffle.partitions", "201")  # default is 200
        .set("spark.sql.adaptive.enabled", "true")
        .set("spark.sql.adaptive.coalescePartitions.enabled", "true")  #
        .set("spark.sql.adaptive.skewJoin.enabled", "true")
        .set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "3")
        .set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "16M")
    )

    spark_session = (
        SparkSession.builder.master("local[8]")
        .config(conf=conf)
        .appName("Read from JDBC tutorial")
        .getOrCreate()
    )

    return spark_session


def trips_data_exploration(spark: SparkSession):
    trips_data = prepare_trips_data(spark=spark)
    trips_data.show(truncate=False)

    trips_data.groupBy("PULocationID").agg(F.count(F.lit(1)).alias("num_rows")).sort(
        F.col("num_rows").desc()
    ).show(truncate=False, n=100)

    location_details_data = spark.read.option("header", True).csv(
        "data/taxi+_zone_lookup.csv"
    )
    location_details_data.show(truncate=False)


def join_on_skewed_data(spark: SparkSession):
    trips_data = prepare_trips_data(spark=spark)
    location_details_data = spark.read.option("header", True).csv(
        "data/taxi+_zone_lookup.csv"
    )

    trips_with_pickup_location_details = trips_data.join(
        location_details_data, F.col("PULocationID") == F.col("LocationID"), "inner"
    )

    trips_with_pickup_location_details.groupBy("Zone").agg(
        F.avg("trip_distance").alias("avg_trip_distance")
    ).sort(F.col("avg_trip_distance").desc()).show(truncate=False, n=1000)

    trips_with_pickup_location_details.groupBy("Borough").agg(
        F.avg("trip_distance").alias("avg_trip_distance")
    ).sort(F.col("avg_trip_distance").desc()).show(truncate=False, n=1000)


# data is split into 25 partitions to reduce skew
# dom_mod col is created based on day of the year from the tpep_pickup_datetime
# column, and then further transformed by taking its modulus with 25
# this ensures that the data is spread across 25 different values
# to distribute the data evenly
def join_on_skewed_data_with_subsplit(spark: SparkSession):
    subsplit_partitions = 25
    trips_data = (
        prepare_trips_data(spark=spark)
        .withColumn("dom_mod", F.dayofyear(F.col("tpep_pickup_datetime")))
        .withColumn("dom_mod", F.col("dom_mod") % subsplit_partitions)
    )

    location_details_data = (
        spark.read.option("header", True)
        .csv("data/taxi+_zone_lookup.csv")
        .withColumn(
            "location_id_alt",
            F.array([F.lit(num) for num in range(0, subsplit_partitions)]),
        )
        .withColumn("location_id_alt", F.explode(F.col("location_id_alt")))
    )

    trips_with_pickup_location_details = trips_data.join(
        location_details_data,
        (F.col("PULocationID") == F.col("LocationID"))
        & (F.col("location_id_alt") == F.col("dom_mod")),
        "inner",
    )

    trips_with_pickup_location_details.groupBy("Zone").agg(
        F.avg("trip_distance").alias("avg_trip_distance")
    ).sort(F.col("avg_trip_distance").desc()).show(truncate=False, n=1000)

    trips_with_pickup_location_details.groupBy("Borough").agg(
        F.avg("trip_distance").alias("avg_trip_distance")
    ).sort(F.col("avg_trip_distance").desc()).show(truncate=False, n=1000)


start_time = time.time()
spark = create_spark_session_with_aqe_disabled()
# spark = create_spark_session_with_aqe_skew_join_enabled()
# spark = create_spark_session_with_aqe_enabled()

trips_data_exploration(spark=spark)

# join_on_skewed_data(spark=spark)
# join_on_skewed_data_with_subsplit(spark=spark)

print(f"Elapsed_time: {(time.time() - start_time)} seconds")
time.sleep(10000)
