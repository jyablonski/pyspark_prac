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
