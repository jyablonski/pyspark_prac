from datetime import datetime, date

from pyspark.sql.functions import (
    avg,
    broadcast,
    col,
    current_timestamp,
    expr,
    lit,
    when,
    md5,
    concat_ws,
    sha2,
)
from pyspark.sql import Row
from pyspark.sql.window import Window

from utils import setup_spark_app, setup_metadata_cols


spark = setup_spark_app(app_name="test")

df = spark.createDataFrame(
    [
        Row(
            id=1, b=2.0, c="string1", d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)
        ),
        Row(
            id=2, b=3.0, c="string2", d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)
        ),
        Row(
            id=4, b=5.0, c="string3", d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0)
        ),
        Row(
            id=1, b=2.0, c="string4", d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0)
        ),
    ]
)

md5_df = (
    df.withColumn("concat_cols", concat_ws("", *["id"]))
    .withColumn("md5_test", md5(col("id").cast("string")))
    .withColumn("row_md5", md5(col("concat_cols")))
    .withColumn("row_sha2", sha2(col("concat_cols"), 256))
)


df2 = setup_metadata_cols(df=df, primary_key_cols=["id"])

new_df = (
    df.withColumn("hash", concat_ws("", *["id"]))
    .withColumn("created_at", current_timestamp())
    .withColumn("test", lit("test"))
)


# filter and then create 1 new column
df2 = df.filter(col("id") >= 2).withColumn("message", lit("Hello World"))


## Case when / if else type columns
# `withColumns to create multiple at id time`
df3 = df.withColumns(
    {
        "test_1": when(col("id") >= 2, 1).otherwise(0),
        "test_3": when(col("id") >= 3, 1).otherwise(0),
        "test_5": when(col("id") >= 5, 1).otherwise(0),
    }
)
df3 = df3.selectExpr(
    "*", "cast(test_5 as string)", "test_1 + test_3 + test_5 as test_sum"
)

df3.collect()


sales_data = [
    (1, 10, 0),
    (2, 20, 0),
    (4, 30, 1),
]

sales_columns = ["id", "sale_price", "is_refund"]

sales_df = spark.createDataFrame(sales_data, schema=sales_columns)

# Perform broadcast inner join
result = df3.join(broadcast(sales_df), df3["id"] == sales_df["id"])

result.collect()


# Perform normal inner join
result2 = df3.join(other=sales_df, on=df3["id"] == sales_df["id"])

result2.collect()


# group by
group_by_aggs_sum = result2.groupBy("is_refund").sum("sale_price")
group_by_aggs_avg = result2.groupBy("is_refund").avg("sale_price")
group_by_aggs_min = result2.groupBy("is_refund").min("sale_price")
group_by_aggs_max = result2.groupBy("is_refund").max("sale_price")


result3 = result2.withColumn(
    "avg_sale_price", avg("sale_price").over(Window.partitionBy("is_refund"))
)

result3.show
