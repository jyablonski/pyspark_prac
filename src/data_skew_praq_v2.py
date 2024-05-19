import os

import requests
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from src.utils import setup_spark_app


# https://mvnrepository.com/artifact/org.postgresql/postgresql
spark_packages = [
    "org.postgresql:postgresql:42.5.4",
]


spark = setup_spark_app(
    app_name="spark_etl_praq",
    spark_packages=spark_packages,
)

df = (
    spark.read.format("jdbc")
    .option(
        "url",
        f"jdbc:postgresql://{os.environ.get('IP')}:5432/{os.environ.get('RDS_DB')}",
    )
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", f"{os.environ.get('RDS_SCHEMA')}.aws_contracts_source")
    .option("user", os.environ.get("RDS_USER"))
    .option("password", os.environ.get("RDS_PW"))
    .load()
)

team_aggs = (
    df.groupby(F.col("team"))
    .agg(F.sum(F.col("season_salary")).alias("sum_contract_value"))
    .sort(F.col("sum_contract_value").desc())
)

team_aggs.write.option("header", "true").mode("overwrite").csv("data/contracts_aggs")
