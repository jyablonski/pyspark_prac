import os

from pyspark import SparkConf
from pyspark.sql import SparkSession

# https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-iceberg-use-spark-cluster.html
conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.1') # basically most recent version as my spark version
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
conf.set('spark.hadoop.fs.s3a.access.key', os.environ.get('aws_access_key_id'))
conf.set('spark.hadoop.fs.s3a.secret.key', os.environ.get('aws_secret_access_key'))
conf.set("spark.jars.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
conf.set("spark.sql.catalog.dev", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.dev.type", "hadoop")
conf.set("spark.sql.catalog.dev.warehouse", "s3://jyablonski-iceberg/jyablonski_db/")

# building it via a config rather than a raw .appName()
spark = SparkSession.builder.config(conf=conf).getOrCreate()

data = spark.createDataFrame([
 ("100", "2015-01-01", "2015-01-01T13:51:39.340396Z"),
 ("101", "2015-01-01", "2015-01-01T12:14:58.597216Z"),
 ("102", "2015-01-01", "2015-01-01T13:51:40.417052Z"),
 ("103", "2015-01-01", "2015-01-01T13:51:40.519832Z")
],["id", "creation_date", "last_update_time"])

## Write a DataFrame as a Iceberg dataset to the Amazon S3 location.
spark.sql("""CREATE TABLE IF NOT EXISTS dev.db.iceberg_table (id string,
creation_date string,
last_update_time string)
USING iceberg
location 's3://DOC-EXAMPLE-BUCKET/example-prefix/db/iceberg_table'""")

data.writeTo("dev.jyablonski_db.iceberg_table").append()


df = spark.read.format("iceberg").load("dev.jyablonski_db.iceberg_table")
df.show()