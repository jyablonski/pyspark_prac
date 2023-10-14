from datetime import datetime
import os

import pandas as pd
import requests
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from src.utils import setup_spark_app


# Create a SparkSession object
spark = setup_spark_app(app_name="spark_http_test")

api = "https://api.jyablonski.dev/game_types"
response = requests.get(api)

df = spark.createDataFrame(response.json())
