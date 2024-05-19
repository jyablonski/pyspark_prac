from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp, md5


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


def setup_metadata_cols(df: DataFrame, primary_key_cols: list[str]) -> DataFrame:
    """
    Function to add Hash + Timestamp Columns onto an existing DataFrame

    Args:
        df (DataFrame): PySpark DataFrame Object to modify

        primary_key_cols (list[str]): List of columns to use as primary key.
            Can be 1 or more columns

    Returns:
        DataFrame: PySpark DataFrame with new columns added

    Example:
        >>> df2 = setup_metadata_cols(df=df, primary_key_cols=["id"])
    """
    # turn string into list of strings if that's what was passed in
    if isinstance(primary_key_cols, str):
        primary_key_cols = [primary_key_cols]

    new_df = df.withColumn("hash", concat_ws("", *primary_key_cols)).withColumn(
        "created_at", current_timestamp()
    )

    return new_df


def get_spark_config(spark: SparkSession) -> list[tuple[str, str]]:
    """
    Small Helper Function to print Spark Config Parameters set on the
    provided Spark Session Object because it's a mfer to remember what
    the command is every time.

    Args:
        spark (SparkSession): Spark Session Object

    Returns:
        list[tuple[str, str]]: List of Tuples with Config Parameters

    """
    config = spark.sparkContext.getConf().getAll()
    print(config)

    return config
