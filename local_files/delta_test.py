import os

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip
import shutil

# this Clears any previous runs
shutil.rmtree("/tmp/delta-table", ignore_errors=True)

conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.2') # basically most recent version as my spark version
conf.set('spark.jars.packages', 'io.delta:delta-core_2.12:2.0.0') 
conf.set('spark.jars.packages', 'com.amazonaws:aws-java-sdk-s3:1.12.243')
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
data.write.format("delta").save("/tmp/delta-table", mode = 'OVERWRITE')

spark.range(5).write.format("delta").save("s3a://jacobsbucket97-dev/spark_delta/delta_table1.delta")