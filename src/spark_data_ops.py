from datetime import datetime
import os
import numpy as np
import pyspark.pandas as ps
import pyspark.sql.functions as F
from pyspark.sql import SparkSession


# https://mvnrepository.com/artifact/org.postgresql/postgresql
spark_packages = [
    "org.postgresql:postgresql:42.5.4",
    "org.apache.hadoop:hadoop-aws:3.3.1",
]

# Create a SparkSession object
spark = (
    SparkSession.builder.appName("spark_data_ops")
    .config("spark.jars.packages", ",".join(spark_packages))
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("aws_access_key_id"))
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("aws_secret_access_key"))
    .getOrCreate()
)

# Read odds table from PostgreSQL
odds = (
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

odds.printSchema()
odds.show(20)

# Read latest boxscores from S3
today = datetime.now().date()

boxscores = spark.read.parquet(
    f"s3a://jyablonski-nba-elt-prod/boxscores/validated/year=2024/month=04/boxscores-2024-04-02.parquet",
    inferSchema=True,
    header=True,
)
boxscores.show(20)

boxscores_pandas = boxscores.toPandas()
print(type(boxscores_pandas))  # pure pandas df

boxscores_pandas_to_spark = ps.from_pandas(boxscores_pandas)
print(boxscores_pandas_to_spark)
print(type(boxscores_pandas_to_spark))  # pyspark pandas df

# same as toPandas()
boxscores_pandas2 = boxscores.to_pandas_on_spark()
print(boxscores_pandas2.dtypes)
print(type(boxscores_pandas2))  # pyspark pandas df

# create a pyspark pandas df from scratch
psdf = ps.DataFrame(
    {
        "A": ["foo", "bar", "foo", "bar", "foo", "bar", "foo", "foo"],
        "B": ["one", "one", "two", "three", "two", "two", "one", "three"],
        "C": np.random.randn(8),
        "D": np.random.randn(8),
    }
)

print(psdf.groupby("A").sum())
print(type(psdf))

# transform boxscores using spark
# filter >= 35 pts, order by that desc, and then create a case when statement for >= 40 pts
boxscores_filtered = (
    boxscores.filter("pts >= 15")
    .sort("pts", ascending=False)
    .withColumn(
        "is_lotta_pts", F.when(F.col("pts") >= 40, "yeaman shore").otherwise("yikes")
    )  # couple different ways to do the filter statement
    .filter("team == 'BOS'")
)

boxscores_filtered.show()

boxscores_filtered = (
    boxscores.filter(F.col("pts") >= 15)
    .sort(F.col("pts"), ascending=False)
    .withColumn(
        "is_lotta_pts", F.when(F.col("pts") >= 40, "yeaman shore").otherwise("yikes")
    )  # couple different ways to do the filter statement
    .filter(F.col("team") == "BOS")
)

boxscores_filtered.show()

# write to s3
boxscores_filtered.coalesce(1).write.parquet(
    "s3a://jyablonski-iceberg/spark_test/parquet/"
)
boxscores_filtered.coalesce(1).write.json("s3a://jyablonski-iceberg/spark_test/json/")
boxscores_filtered.coalesce(1).write.csv("s3a://jyablonski-iceberg/spark_test/csv/")

# transform boxscores using spark sql
boxscores.createOrReplaceTempView("boxscores_sql")

boxscores_sql = spark.sql(
    """
    select
        *,
        case when pts >= 40 then 'yeaman shore' else 'yikes' end as is_lotta_pts
    from boxscores_sql
    where team = 'DAL' and pts >= 30 and player = 'Luka Doncic'
        """
)

boxscores_sql.show()

# write to s3
boxscores_sql.coalesce(1).write.parquet(
    "s3a://jyablonski-iceberg/spark_sql_test/parquet/"
)
boxscores_sql.coalesce(1).write.json("s3a://jyablonski-iceberg/spark_sql_test/json/")
boxscores_sql.coalesce(1).write.csv("s3a://jyablonski-iceberg/spark_sql_test/csv/")

# coalesce(1) means write to only 1 file

spark.stop()
