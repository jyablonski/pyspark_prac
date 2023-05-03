from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import time
import os

# https://github.com/apache/iceberg/issues/3829
spark_packages = [
    "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.1.0",
    "software.amazon.awssdk:bundle:2.20.18",
    "software.amazon.awssdk:url-connection-client:2.20.18",
    "org.apache.hadoop:hadoop-aws:3.3.2",
]

catalog_name = "iceberg_dev"

# this is used for ACID transactions to lock the table from being changed while an insert update or delete is in progress.
# .config(f'spark.sql.catalog.{catalog_name}.lock-impl', 'org.apache.iceberg.aws.glue.DynamoLockManager') \
# .config(f'spark.sql.catalog.{catalog_name}.lock.table', f'{catalog_name}') \
# .config(f'spark.sql.catalog.{catalog_name}.type', 'hadoop') \
spark = (
    SparkSession.builder.config("spark.jars.packages", ",".join(spark_packages))
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("aws_access_key_id"))
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("aws_secret_access_key"))
    .config(
        f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog"
    )
    .config(f"spark.sql.catalog.{catalog_name}.warehouse", f"$PWD/warehouse")
    .config(f"spark.sql.catalog.{catalog_name}.type", "hadoop")
    .config(f"spark.sql.defaultCatalog", catalog_name)
    .getOrCreate()
)

subName = "product.testtable"
tableName = f"{catalog_name}.{subName}"
spark.sql(f"DROP TABLE IF EXISTS {tableName}")
spark.sql(f"CREATE TABLE {tableName} (id bigint NOT NULL, data string) USING iceberg")
spark.sql(f"INSERT INTO TABLE {tableName} VALUES (1, 'a')")
time.sleep(3)
spark.sql(f"INSERT INTO TABLE {tableName} VALUES (2, 'b')")
expiretime = str(
    spark.sql(f"SELECT * FROM {tableName}.snapshots").tail(1)[0]["committed_at"]
)
spark.sql(
    f"CALL {catalog_name}.system.expire_snapshots('{subName}', TIMESTAMP '{expiretime}')"
).show()


data = spark.createDataFrame(
    [
        ("100", "2015-01-01", "2015-01-01T13:51:39.340396Z"),
        ("101", "2015-01-01", "2015-01-01T12:14:58.597216Z"),
        ("102", "2015-01-01", "2015-01-01T13:51:40.417052Z"),
        ("103", "2015-01-01", "2015-01-01T13:51:40.519832Z"),
    ],
    ["id", "creation_date", "last_update_time"],
)

# STEP 3 Write it back to S3
# df5.write.mode("overwrite").format("csv").save(s3_dest_path)
data.coalesce(1).write.mode("overwrite").parquet(
    "s3a://jyablonski-iceberg/pyspark/test.parquet"
)

df = spark.read.parquet(
    "s3a://jyablonski-iceberg/reddit_comment_data-2023-03-06.parquet",
    inferSchema=True,
    header=True,
)

## Write a DataFrame as a Iceberg dataset to the Amazon S3 location.
spark.sql(
    """CREATE TABLE IF NOT EXISTS spark_catalog.iceberg_test.iceberg_table (id string,
creation_date string,
last_update_time string)
USING iceberg"""
)
