import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, expr, filter, max
from pyspark.sql.types import DoubleType, StructField, StringType

from src.utils import setup_spark_app

# https://mvnrepository.com/artifact/org.postgresql/postgresql
spark_packages = [
    "org.postgresql:postgresql:42.5.4",
]

spark = setup_spark_app(app_name="praq_v2", spark_packages=spark_packages)

df = (
    spark.read.format("jdbc")
    .option(
        "url",
        f"jdbc:postgresql://{os.environ.get('IP')}:5432/{os.environ.get('RDS_DB')}",
    )
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", f"{os.environ.get('RDS_SCHEMA')}.aws_boxscores_source")
    .option("user", os.environ.get("RDS_USER"))
    .option("password", os.environ.get("RDS_PW"))
    .load()
)

schema = df.schema
schema_player = schema["player"]
schema_0 = schema[0]

if not isinstance(schema_player.dataType, DoubleType):
    raise ValueError(f"Column has wrong Data Type, expected DoubleType")


max_date_all = df.groupby(col("team")).agg(max(col("scrape_date")))
max_date_gsw = (
    df.groupby(col("team")).agg(max(col("scrape_date"))).filter(col("team") == "GSW")
)

max_date_all = df.groupby([col("team"), col("type")]).agg(max(col("scrape_date")))
