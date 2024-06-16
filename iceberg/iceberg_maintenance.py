import time
import os

from pyspark.sql import SparkSession
from src.utils import move_to_iceberg


# https://github.com/apache/iceberg/issues/3829
# no clue which one of these were / werent needed but wtf ever
spark_packages = [
    "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2",
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
    .config(
        f"spark.sql.catalog.{catalog_name}.warehouse",
        f"s3a://jyablonski2-iceberg/{catalog_name}",
    )
    .config(f"spark.sql.catalog.{catalog_name}.type", "hadoop")
    .config(f"spark.sql.defaultCatalog", catalog_name)
    .getOrCreate()
)

# list databases
spark.catalog.listDatabases()

# list tables in the product schema
spark.catalog.listTables("iceberg_dev.product")

tables = spark.sql("show tables from iceberg_dev.product")
