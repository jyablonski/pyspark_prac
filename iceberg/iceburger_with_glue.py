import os

from pyspark.sql import SparkSession
from src.utils import move_to_iceberg

spark_packages = [
    "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2",
    "org.apache.iceberg:iceberg-aws-bundle:1.4.2",
    "software.amazon.awssdk:bundle:2.20.18",
    "software.amazon.awssdk:url-connection-client:2.20.18",
    "org.apache.hadoop:hadoop-aws:3.3.2",
]

catalog_name = "nba_elt_iceberg"
catalog_prefix = "prod"

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
    .config(f"spark.sql.defaultCatalog", catalog_name)
    .config(
        f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog"
    )
    .config(
        f"spark.sql.catalog.{catalog_name}.warehouse",
        f"s3a://jyablonski2-iceberg/{catalog_prefix}",
    )
    .config(
        f"spark.sql.catalog.{catalog_name}.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog",
    )
    .config(
        f"spark.sql.catalog.{catalog_name}.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO",
    )
    .getOrCreate()
)


# this mf format dude
# it's `iceberg_catalog_name`.`glue_database_name`.`your_table`
table_name = f"{catalog_name}.{catalog_name}.test_table1"
spark.sql(f"DROP TABLE IF EXISTS {table_name};")
spark.sql(f"CREATE TABLE {table_name} (id bigint NOT NULL, data string) USING iceberg")


# move s3 partitioned files to apache iceburger
move_to_iceberg(
    spark=spark,
    s3_source_data="s3a://jyablonski-nba-elt-prod/reddit_comment_data/",
    iceberg_schema=catalog_name,
    iceberg_table="reddit_comment_data",
    table_partition_col="scrape_date",
)

# list all tables
spark.sql(f"show tables in {catalog_name}.{catalog_name};").show()

# pull data from the pbp table
pbp_data = spark.table("pbp_data")
