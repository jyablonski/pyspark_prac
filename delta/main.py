import os

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip
import shutil

# https://stackoverflow.com/questions/61769528/writing-delta-lake-to-aws-s3-without-databricks

spark_packages = [
    "org.apache.hadoop:hadoop-aws:3.3.1",
    "io.delta:delta-core_2.12:2.0.2",
]

# this clears any local stuff from previous runs
shutil.rmtree("/tmp/delta-table", ignore_errors=True)

conf = SparkConf()
conf.set('spark.jars.packages', ','.join(spark_packages))
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
conf.set('spark.hadoop.fs.s3a.access.key', os.environ.get('aws_access_key_id'))
conf.set('spark.hadoop.fs.s3a.secret.key', os.environ.get('aws_secret_access_key'))
conf.set('spark.sql.extensions', "io.delta.sql.DeltaSparkSessionExtension")
conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = SparkSession.builder.appName("delta_praq") \
   .config(conf = conf) \
   .getOrCreate()

print("############# Creating a table ###############")
data = spark.range(0, 5)

# write locally
data.write.format("delta").save("/tmp/delta-table", mode = 'OVERWRITE')

# write to s3 bucket
data.write.format("delta").mode("overwrite").save("s3a://jyablonski-delta/delta_test/")