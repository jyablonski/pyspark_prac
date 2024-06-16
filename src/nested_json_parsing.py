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
    explode_outer,
)
from pyspark.sql import Row
from pyspark.sql.types import (
    BooleanType,
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    ArrayType,
)

from pyspark.sql.window import Window

from src.utils import setup_spark_app, setup_metadata_cols


spark = setup_spark_app(app_name="test")

orders_schema = StructType(
    [
        StructField("user_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField(
            "address",
            StructType(
                [
                    StructField("street", StringType(), True),
                    StructField("city", StringType(), True),
                    StructField("state", StringType(), True),
                    StructField("postal_code", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "orders",
            ArrayType(
                StructType(
                    [
                        StructField("order_id", IntegerType(), True),
                        StructField(
                            "date", StringType(), True
                        ),  # Assuming date is stored as string for simplicity
                        StructField("total", DoubleType(), True),
                        StructField(
                            "items",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("product_id", StringType(), True),
                                        StructField("name", StringType(), True),
                                        StructField("quantity", IntegerType(), True),
                                        StructField("price", DoubleType(), True),
                                    ]
                                )
                            ),
                            True,
                        ),
                    ]
                )
            ),
            True,
        ),
        StructField(
            "preferences",
            StructType(
                [
                    StructField("newsletter", BooleanType(), True),
                    StructField("sms_notifications", BooleanType(), True),
                    StructField("preferred_payment_method", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)

# if data is properly quoted then you can use multi-line parser
# this requires reading the data onto a single executor
df = (
    spark.read.option(key="multiline", value=True)
    .format(source="json")
    .schema(schema=orders_schema)
    .load(path="src/data/nested_orders.json")
)

df.printSchema()

df.show(truncate=False)

# can flatten out simple structs like this
# "address": {
#     "street": "321 Poplar St",
#     "city": "Redtown",
#     "state": "NC",
#     "postal_code": "27601"
# },
nested_json = (
    df.withColumn("street", df["address"]["street"])
    .withColumn("city", df["address"]["city"])
    .withColumn("state", df["address"]["state"])
    .withColumn("postal_code", df["address"]["postal_code"])
    .withColumn("newsletter", df["preferences"]["newsletter"])
    .withColumn("sms_notifications", df["preferences"]["sms_notifications"])
    .withColumn(
        "preferred_payment_method", df["preferences"]["preferred_payment_method"]
    )
    .drop("address", "preferences")
)

nested_json.show(truncate=False)


# unnest til we get 1 row per order id mfer
orders_unnested = (
    nested_json.select(
        "user_id",
        "name",
        "email",
        "street",
        "city",
        "state",
        "postal_code",
        explode_outer("orders").alias("order"),
    )
    .select(
        "user_id",
        "name",
        "email",
        "street",
        "city",
        "state",
        "postal_code",
        "order.order_id",
        "order.date",
        "order.total",
        explode_outer("order.items").alias("item"),
    )
    .select(
        "order_id",
        "user_id",
        "name",
        "email",
        "street",
        "city",
        "state",
        "postal_code",
        "date",
        "total",
        "item.product_id",
        "item.name",
        "item.quantity",
        "item.price",
    )
).orderBy("order_id", "user_id")

orders_unnested.show(truncate=False)