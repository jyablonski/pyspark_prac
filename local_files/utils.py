from pyspark.sql import SparkSession


def setup_spark_app(app_name: str, spark_packages=None | list[str], local=True):
    """
    Set up a PySpark application.

    Parameters:
        app_name (str): Name of the Spark application.

        spark_packages (list): List of Spark packages to include.

        local (bool): Whether to connect to a local Spark cluster or not.

    Returns:
        SparkSession: PySpark SparkSession object.
    """

    # Configure Spark
    spark_builder = SparkSession.builder.appName(app_name)

    if spark_packages:
        spark_builder.config("spark.jars.packages", ",".join(spark_packages))

    if local:
        # Set up for local mode
        spark_builder.master("local[*]")
    else:
        # Set up for connecting to a remote Spark cluster
        # Update the master URL accordingly for your remote Spark cluster
        spark_builder.master("spark://<master_url>:<master_port>")

    spark = spark_builder.getOrCreate()
    return spark
