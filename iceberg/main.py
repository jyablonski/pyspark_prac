import os

from pyspark import SparkConf
from pyspark.sql import SparkSession

# https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-iceberg-use-spark-cluster.html
# spark 3.2, 
conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.1') # basically most recent version as my spark version
conf.set('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.1.0')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
conf.set('spark.hadoop.fs.s3a.access.key', os.environ.get('aws_access_key_id'))
conf.set('spark.hadoop.fs.s3a.secret.key', os.environ.get('aws_secret_access_key'))
conf.set("spark.jars.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
conf.set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
conf.set('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog')
conf.set('spark.sql.catalog.spark_catalog.type', 'hadoop')
conf.set("spark.sql.catalog.spark_catalog.warehouse", "s3a://jyablonski-iceberg/iceberg_test")
conf.set("spark.sql.catalog.spark_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO" )
# conf.set('spark.sql.defaultCatalog', 'local')
# building it via a config rather than a raw .appName()
spark = SparkSession.builder.config(conf=conf).getOrCreate()
print(os.environ.get('aws_access_key_id'))

data = spark.createDataFrame([
 ("100", "2015-01-01", "2015-01-01T13:51:39.340396Z"),
 ("101", "2015-01-01", "2015-01-01T12:14:58.597216Z"),
 ("102", "2015-01-01", "2015-01-01T13:51:40.417052Z"),
 ("103", "2015-01-01", "2015-01-01T13:51:40.519832Z")
],["id", "creation_date", "last_update_time"])

# STEP 3 Write it back to S3
# df5.write.mode("overwrite").format("csv").save(s3_dest_path)
data.coalesce(1).write.parquet('s3a://jacobsbucket97-dev/pyspark/test.parquet')

df = spark.read.parquet('s3a://jyablonski-iceberg/reddit_comment_data-2023-03-06.parquet', inferSchema = True, header = True)

## Write a DataFrame as a Iceberg dataset to the Amazon S3 location.
spark.sql("""CREATE TABLE IF NOT EXISTS spark_catalog.iceberg_test.iceberg_table (id string,
creation_date string,
last_update_time string)
USING iceberg""")
          
# spark.sql("""CREATE TABLE IF NOT EXISTS dev.db.iceberg_table (id string,
# creation_date string,
# last_update_time string)
# USING iceberg
# location 's3://jyablonski-iceberg/iceberg_table'""")

data.writeTo("dev.jyablonski_db.iceberg_table").append()


df = spark.read.format("iceberg").load("dev.jyablonski_db.iceberg_table")
df.show()