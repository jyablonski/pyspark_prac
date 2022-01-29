import pyspark
from pyspark.sql import Row
import pandas as pd
from datetime import datetime, date

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Practice").getOrCreate()

# df = spark.createDataFrame([
#     (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
#     (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
#     (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
# ], schema='a long, b double, c string, d date, e timestamp')


# df.write.parquet('nba_tweets.parquet', compression = 'snappy')

df = read.parquet('nba_tweets.parquet')
df