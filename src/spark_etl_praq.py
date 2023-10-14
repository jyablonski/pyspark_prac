import os

import requests
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from src.utils import setup_spark_app

s3_bucket = "s3a://jyablonski-test-bucket123/pyspark/"

etl_packages = [
    "org.apache.hadoop:hadoop-aws:3.3.1",
]

etl_config = {
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.hadoop.fs.s3a.access.key": os.environ.get("aws_access_key_id"),
    "spark.hadoop.fs.s3a.secret.key": os.environ.get("aws_secret_access_key"),
}


spark = setup_spark_app(
    app_name="spark_etl_praq",
    spark_packages=etl_packages,
    spark_config=etl_config,
)



api = "https://api.jyablonski.dev/injuries"
response = requests.get(api)

df = spark.createDataFrame(response.json()) \
    .withColumn("scrape_timestamp", F.current_timestamp())


df.coalesce(1).write.parquet(f"{s3_bucket}/parquet")
df.coalesce(1).write.json(f"{s3_bucket}/json")