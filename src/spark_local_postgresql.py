from datetime import datetime
import os

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from src.utils import setup_spark_app

# https://mvnrepository.com/artifact/org.postgresql/postgresql
spark_packages = [
    "org.postgresql:postgresql:42.5.4",
]

# Create a SparkSession object
spark = setup_spark_app(app_name="spark_postgres_test", spark_packages=spark_packages)

df = (
    spark.read.format("jdbc")
    .option(
        "url",
        f"jdbc:postgresql://localhost:5432/postgres",
    )
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", f"source.customers")
    .option("user", "postgres")
    .option("password", "postgres")
    .load()
)

df.show()
