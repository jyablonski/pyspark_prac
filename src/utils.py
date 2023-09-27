from pyspark.sql import SparkSession


def setup_spark_app(
    app_name: str,
    spark_packages: list[str] | None = None,
    spark_host: str | None = None,
):
    """
    Set up a PySpark application.

    Parameters:
        app_name (str): Name of the Spark application.

        spark_packages (None | str): Optional list of Spark packages to include.

        spark_host (None | str): Optional Parameter to specify the Spark Host if
            connecting to a Remote Cluster.  If not specified then a local Spark
            Session is built.

    Returns:
        SparkSession: PySpark SparkSession object

    Example:
        >>> spark = setup_spark_app(f"test")
    """

    # Configure Spark
    spark_builder = SparkSession.builder.appName(app_name)

    if spark_packages:
        spark_builder.config("spark.jars.packages", ",".join(spark_packages))

    if spark_host:
        print(f"Building SparkSession to Remote Spark Cluster at spark://{spark_host}")

        # spark://<master_url>:<master_port>
        spark_builder.master(f"spark://{spark_host}>")
    else:
        print(f"Building SparkSession on Local Machine")
        # Set up for local mode
        spark_builder.master("local[*]")

    spark = spark_builder.getOrCreate()
    return spark
