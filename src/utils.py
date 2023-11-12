from pyspark.sql import SparkSession


# spark.sparkContext._conf.getAll()
def setup_spark_app(
    app_name: str,
    spark_packages: list[str] | None = None,
    spark_config: dict | None = None,
    spark_host: str | None = None,
) -> SparkSession:
    """
    Set up a PySpark application.

    Parameters:
        app_name (str): Name of the Spark application.

        spark_packages (None | str): Optional list of Spark packages to include.

        spark_config (None | dict): Optional dictionary to specify Config Options

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

    if spark_config:
        for config, config_value in spark_config.items():
            spark_builder.config(config, config_value)

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


def move_to_iceberg(
    spark: SparkSession,
    s3_source_data: str,
    iceberg_schema: str,
    iceberg_table: str,
    table_partition_col: str,
) -> None:
    print(f"Recursively Loading Data from {s3_source_data}")
    df = spark.read.parquet(s3_source_data)

    df = df.orderBy(table_partition_col)
    df.createOrReplaceTempView(iceberg_table)

    print(f"Creating Table {iceberg_schema}.{iceberg_table}")
    spark.sql(
        f"""
        create table {iceberg_schema}.{iceberg_schema}.{iceberg_table} using iceberg as
        select * from {iceberg_table};"""
    )
    pass
