from datetime import datetime
import os

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window


# https://mvnrepository.com/artifact/org.postgresql/postgresql
spark_packages = [
    "org.postgresql:postgresql:42.5.4",
]

# Create a SparkSession object
spark = (
    SparkSession.builder.appName("ReadFromPostgreSQL")
    .config("spark.jars.packages", ",".join(spark_packages))
    .getOrCreate()
)

df = (
    spark.read.format("jdbc")
    .option(
        "url",
        f"jdbc:postgresql://{os.environ.get('IP')}:5432/{os.environ.get('RDS_DB')}",
    )
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", f"{os.environ.get('RDS_SCHEMA')}.aws_odds_source")
    .option("user", os.environ.get("RDS_USER"))
    .option("password", os.environ.get("RDS_PW"))
    .load()
)

# boolean filter, and IN () filter
df_filter = df.filter((df.team == "DAL") | (df.team == "LA")).show()
df_filter = df.filter(F.col("team").isin("DAL", "LA")).show()

# start and end with
df_filter = df.filter(F.col("team").startswith("L")).show()
df_filter = df.filter(F.col("team").endswith("S")).show()

# aggregate
df_agg = df.groupby("team").avg("moneyline").alias("avg_moneyline").show()

# aggreagte
df = (
    df.withColumn("avg_moneyline", F.avg("moneyline").over(Window.partitionBy("team")))
    .withColumn("avg_moneyline", F.round("avg_moneyline", 2))
    .filter(F.col("date").between("2022-12-01", "2022-12-31"))
    .withColumn("is_favorite", F.when(F.col("moneyline") < 0, 1).otherwise(0))
    .withColumn("is_underdog", F.when(df.moneyline > 0, 1).otherwise(0))
)

df.show()

df2 = df.collect()
spark.stop()
