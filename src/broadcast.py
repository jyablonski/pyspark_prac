from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# Initialize the Spark session
spark = SparkSession.builder.appName("Broadcast Join Example").getOrCreate()

# Create DataFrames from sample data
sales_data = [(1, 101, 2), (2, 102, 1), (3, 103, 3), (4, 101, 1), (5, 104, 4)]
products_data = [
    (101, "Learn C++", 10),
    (102, "Mobile: X1", 20),
    (103, "LCD", 30),
    (104, "Laptop", 40),
]

sales_columns = ["order_id", "product_id", "quantity"]
products_columns = ["product_id", "product_name", "price"]

sales_df = spark.createDataFrame(sales_data, schema=sales_columns)
products_df = spark.createDataFrame(products_data, schema=products_columns)

# Perform broadcast join
result = sales_df.join(
    broadcast(products_df), sales_df["product_id"] == products_df["product_id"]
)

result = sales_df.join(products_df, sales_df["product_id"] == products_df["product_id"])

# Show result
result.show()
