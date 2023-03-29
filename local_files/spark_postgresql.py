import os
from pyspark.sql import SparkSession

# https://mvnrepository.com/artifact/org.postgresql/postgresql
spark_packages = [
    "org.postgresql:postgresql:42.5.4",
]

# Create a SparkSession object
spark = SparkSession.builder.appName("ReadFromPostgreSQL") \
    .config('spark.jars.packages', ','.join(spark_packages)) \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://{os.environ.get('IP')}:5432/{os.environ.get('RDS_DB')}") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", f"{os.environ.get('RDS_SCHEMA')}.aws_odds_source") \
    .option("user", os.environ.get('RDS_USER')) \
    .option("password", os.environ.get('RDS_PW')) \
    .load()

df.printSchema()
df.show(20)

df2 = df.collect()

spark.stop()




