import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
    return spark


@pytest.fixture()
def full_df_fixture(spark_session):
    test_df = spark_session.createDataFrame(
        data=[
            ("red", "Jacob", 5),
            ("red", "Jacob", 50),
            ("green", "Jacob2", 20),
            ("black", "Jacob3", 1000),
        ],
        schema=["color", "owner", "price"],
    )

    return test_df


@pytest.fixture()
def df_dtype_fixture(spark_session):
    # same df i'm just making the price col as a string to test changing the data type
    test_df = spark_session.createDataFrame(
        data=[("Jacob", "5"), ("Jacob1", "50"), ("Jacob2", "20"), ("Jacob3", "1000")],
        schema=["owner", "price"],
    )

    return test_df


@pytest.fixture()
def customers_fixture(spark_session):
    customers = spark_session.createDataFrame(
        data=[
            (1, "Alice", 34, 1, "2017-08-02 16:16:11"),
            (2, "Bob", 45, 20, "2017-08-03 16:16:13"),
            (3, "Charlie", 56, 30, "2017-08-04 16:16:19"),
            (4, "David", 23, 40, "2017-08-04 18:16:15"),
            (5, "Eve", 67, 50, "2017-08-05 16:16:17"),
        ],
        schema=["id", "name", "age", "order_id", "created_at"],
    ).alias("customers")

    return customers


@pytest.fixture()
def orders_fixture(spark_session):
    orders = spark_session.createDataFrame(
        data=[
            (1, "order1", 100, 1),
            (20, "order2", 200, 1),
            (30, "order3", 300, 2),
            (40, "order4", 400, 1),
            (50, "order5", 500, 1),
        ],
        schema=["id", "order_name", "amount", "quantity"],
    ).alias("orders")

    return orders
