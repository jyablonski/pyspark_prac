import time
import os

from pyspark.sql import SparkSession


# https://github.com/apache/iceberg/issues/3829
# no clue which one of these were / werent needed but wtf ever
spark_packages = [
    "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.1.0",
    "software.amazon.awssdk:bundle:2.20.18",
    "software.amazon.awssdk:url-connection-client:2.20.18",
    "org.apache.hadoop:hadoop-aws:3.3.2",
]

catalog_name = "iceberg_dev"

# ngl, holy fuck java sucks complete balls jesus fkn christ man

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
        f"s3a://jyablonski-iceberg/{catalog_name}",
    )
    .config(f"spark.sql.catalog.{catalog_name}.type", "hadoop")
    .config(f"spark.sql.defaultCatalog", catalog_name)
    .getOrCreate()
)

table_name = "product.testtable"
table_id = f"{catalog_name}.{table_name}"
spark.sql(f"DROP TABLE IF EXISTS {table_id}")
spark.sql(f"CREATE TABLE {table_id} (id bigint NOT NULL, data string) USING iceberg")

spark.sql(f"INSERT INTO TABLE {table_id} VALUES (1, 'a')")
time.sleep(3)
spark.sql(f"INSERT INTO TABLE {table_id} VALUES (2, 'b')")

expiretime = str(
    spark.sql(f"SELECT * FROM {table_id}.snapshots").tail(1)[0]["committed_at"]
)
spark.sql(
    f"CALL {catalog_name}.system.expire_snapshots('{table_name}', TIMESTAMP '{expiretime}')"
).show()

# read the whole iceberg table
df_iceburger = spark.read.format("iceberg").load(f"{table_id}").toPandas()

df_iceburger_history = (
    spark.read.format("iceberg").load(f"{table_id}.history").collect()
)
df_iceburger_snapshots = (
    spark.read.format("iceberg").load(f"{table_id}.snapshots").collect()
)
df_iceburger_files = spark.read.format("iceberg").load(f"{table_id}.files").collect()

# i never got this working - dont think you're supposed to query this way.  use the above ^
df_iceburger2 = spark.read.format("iceberg").load(
    f"s3a://jyablonski-iceberg/{catalog_name}/{table_id}"
)

# read a hardcoded parquet file in s3
df3 = spark.read.parquet(
    "s3a://jyablonski-iceberg/iceberg_dev/product/testtable/data/00000-614-b30da3bf-2b87-4eea-9147-b690f6b6f667-00001.parquet"
).toPandas()
