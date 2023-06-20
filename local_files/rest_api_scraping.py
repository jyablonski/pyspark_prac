import os

import requests
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

spark = SparkSession.builder.master("local[*]").getOrCreate()

api = "https://api.jyablonski.dev/game_types"

response = requests.get(api)

rdd = spark.sparkContext.parallelize([response.text])
df = spark.read.json(rdd)

# new agg column grouping by 1 column
df2 = df \
    .withColumn("games_avg", F.avg("n").over(Window.partitionBy('type')))

# new agg column grouping by 2 columns
df3 = df \
    .withColumn("games_avg", F.avg("n").over(Window.partitionBy(['type', 'game_type'])))

# filter dataframe
df3_filtered = df3.filter(df3.games_avg == 24.0)

# create new column
df4 = df3 \
    .withColumn("fake_col", F.col("n") + 35) \

# only select a few columns
df4_only_two_cols = df4.select(["explanation", "fake_col"])

# create new agg df
# new columns get aliased with their column name as an argument in the `.agg` function
df_aggs = df3.groupby(["type", "game_type"]) \
    .agg(F.sum('n').alias('sum_n'))

# create new agg df - with multiple aggs from same `.agg` function
df_aggs = df3.groupby(["type", "game_type"]) \
    .agg(F.sum('n').alias('sum_n'), 
    F.avg('n').alias('avg_n'), 
    F.max('n').alias('max_n'), 
    F.count('n').alias('count_n'))

# filter dataframe with >= and `F.col` syntax
df3_filtered_greater_than = df3.filter(F.col('games_avg') >= 300)


## SQL
df.createOrReplaceTempView("game_types")
clutch_games = spark.sql(f"select * from game_types where game_type == 'Clutch Game';")
clutch_games.show()


##
df2 = spark.read.json(response.json())