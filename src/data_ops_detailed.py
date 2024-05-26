from pyspark.sql.functions import (
    avg,
    broadcast,
    col,
    current_timestamp,
    max,
    rank,
    lit,
    when,
    window,
)
from pyspark.sql import DataFrame, SparkSession, Window


from src.utils import setup_spark_app

spark = setup_spark_app("test")

# this is fucking absurd
# https://stackoverflow.com/questions/73395718/join-dataframes-and-rename-resulting-columns-with-same-names
customers = spark.createDataFrame(
    data=[
        (1, "Alice", 34, 1, "2017-08-02 16:16:11"),
        (2, "Bob", 45, 20, "2017-08-03 16:16:13"),
        (3, "Charlie", 56, 30, "2017-08-04 16:16:19"),
        (4, "David", 23, 40, "2017-08-04 18:16:15"),
        (5, "Eve", 67, 50, "2017-08-05 16:16:17"),
    ],
    schema=["id", "name", "age", "order_id", "created_at"],
).alias("customers")

orders = spark.createDataFrame(
    data=[
        (1, "order1", 100, 1),
        (20, "order2", 200, 1),
        (30, "order3", 300, 2),
        (40, "order4", 400, 1),
        (50, "order5", 500, 1),
    ],
    schema=["id", "order_name", "amount", "quantity"],
).alias("orders")

# this is easier to test but objectively worse to read than sql
# window function require that weird `Window` function to be used
# look at how many times col and alias have to be repeated as opposed to select customers.id as customer_id etc.
combo_df = (
    customers.join(other=orders, on=customers["order_id"] == orders["id"])
    .select(
        col("customers.id").alias("customer_id"),
        col("orders.id").alias("order_id"),
        col("orders.order_name").alias("order_name"),
        col("orders.amount").alias("order_amount"),
        col("orders.quantity"),
        col("customers.created_at"),
    )
    .withColumn("total_amount", col("order_amount") * col("quantity"))
    .withColumn(
        "amount_type", when(col("order_amount") >= 300, "high").otherwise("low")
    )
    .withColumn("currency_type", lit("USD"))
    .withColumn(
        "order_amount_rank", rank().over(Window.orderBy(col("order_amount").desc()))
    )
    .withColumn("pipeline_ts", current_timestamp())
    .orderBy("order_id")
)


# Show the resulting DataFrame
combo_df.show()

combo_df.createOrReplaceTempView("combo_df")

# i guess you cant use the alias method with agg
max_timestamp = (
    combo_df.select("created_at").agg({"created_at": "max"}).alias("max_timestamp")
)
# column is still called `max(created_at)` not `max_timestamp`
max_timestamp.show()

# this is the correct way to use alias
max_timestamp = combo_df.select("created_at").agg(
    max("created_at").alias("max_timestamp")
)
max_timestamp.show()


# only grab latest data
max_data = combo_df.join(
    other=broadcast(max_timestamp), on=col("created_at") == col("max_timestamp")
)
max_data.show()

# read and write it back from disk
max_data.write.mode("overwrite").parquet("data/max_data")
max_data_v2 = spark.read.parquet("data/max_data")


combo_df_sql = spark.sql(
    """
    select * from combo_df
    """
).show()

max_data_sql = spark.sql(
    """
    with max_date as (
        select max(created_at) as max_timestamp
        from combo_df
    )

    select combo_df.*
    from combo_df
        inner join max_date
            on combo_df.created_at = max_date.max_timestamp
    """
).show()

# no qualify sucks lol
qualify = spark.sql(
    """
    select
        *,
        rank() over (order by order_amount desc) as rank
    from combo_df
    -- qualify rank = 1
    """
).show()


# the function route
def combine_orders(customers: DataFrame, orders: DataFrame) -> DataFrame:
    """
    Process customers and orders DataFrames to produce the combo_df DataFrame.

    Args:
        customers (DataFrame): A DataFrame of customers.

        orders (DataFrame): A DataFrame of orders.

    Returns:
        DataFrame: A DataFrame with the combined data.

    """
    combo_df = (
        customers.join(other=orders, on=customers["order_id"] == orders["id"])
        .select(
            col("customers.id").alias("customer_id"),
            col("orders.id").alias("order_id"),
            col("orders.order_name").alias("order_name"),
            col("orders.amount").alias("order_amount"),
            col("orders.quantity"),
            col("customers.created_at"),
        )
        .withColumn("total_amount", col("order_amount") * col("quantity"))
        .withColumn(
            "amount_type", when(col("order_amount") >= 300, "high").otherwise("low")
        )
        .withColumn("currency_type", lit("USD"))
        .withColumn(
            "order_amount_rank", rank().over(Window.orderBy(col("order_amount").desc()))
        )
        .withColumn("pipeline_ts", current_timestamp())
        .orderBy("order_id")
    )
    return combo_df


df4 = combine_orders(customers=customers, orders=orders).show()
