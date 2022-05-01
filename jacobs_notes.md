# Spark Entrypoint
`spark = SparkSession.builder.appName("Practice").getOrCreate()` is the entrypoint into Spark.  Before version 2.x.x, it was SparkContext or SQLContext or Hivecontext, but now SparkSession inherits all 3 for backwards compatibility.  You should only ever use `SparkSession`.

SparkContext establishes communication with the cluster and resource managers to coordinate and execute jobs.  Need to set an app name and then use getOrCreate().

`import pyspark`
`from pyspark.sql import SparkSession`
`import pyspark.sql.functions as F` is a basic import that is used for a ton of shit.
spark.conf.set('spark.sql.repl.eagerEval.enabled', True) - will make it so your tables look good in jupyter notebooks

# Building your own data frames

1) First way with tuples
   
test_df = spark_session.createDataFrame(
        data = [
            ('red', 'Jacob', 5),
            ('red', 'Jacob', 50),
            ('green', 'Jacob2', 20),
            ('black', 'Jacob3', 1000)
        ],
        schema = ['color', 'owner', 'price']
    )
allows you to build your own data, passing in lists for each column and then the schema

2) The other way is to do Row from pyspark

from pyspark.sql import Row
df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
])


3) create it by defining StructField objects for each column (it's weird bc it's fucking java m8)

data_schema = [
               StructField('Car', StringType(), True),
               StructField('MPG', DoubleType(), True),
               StructField('Cylinders', IntegerType(), True),
               StructField('Displacement', DoubleType(), True),
               StructField('Horsepower', DoubleType(), True),
               StructField('Weight', DoubleType(), True),
               StructField('Acceleration', DoubleType(), True),
               StructField('Model', StringType(), True),
               StructField('Origin', StringType(), True),
            ]
final_struc = StructType(fields = data_schema)

data = spark.read.csv(
    'cars.csv',
    sep = ';',
    header = True,
    schema = final_struc 
    )

4) 

df1 = spark.createDataFrame([[1, 2, 3]], ["col0", "col1", "col2"])
df2 = spark.createDataFrame([[4, 5, 6]], ["col1", "col2", "col0"])

# AWS S3 Connection
The Below code will automatically download the necessary shit to connect.
I had to download the latest possible versions of whatever from [here](https://hadoop.apache.org/docs/r2.8.0/hadoop-aws/tools/hadoop-aws/index.html#Changing_Authentication_Providers) to get it to work.  -- apparently YOUR version of pyspark needs to match with the hadoop aws version you download.
The access credentials need to be for a IAM user which has S3 Read Access (and write access if you want to write files)

conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.1') # basically most recent version as my spark version
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
conf.set('spark.hadoop.fs.s3a.access.key', os.environ.get('aws_access_key_id'))
conf.set('spark.hadoop.fs.s3a.secret.key', os.environ.get('aws_secret_access_key'))

spark = SparkSession.builder.config(conf=conf).getOrCreate()

and then you can do

df = spark.read.csv('s3a://jacobsbucket97/sample_files/nba_tweets.csv', inferSchema = True, header = True)
df.createTempView('nba_data')
df_s3_spark = spark.sql("SELECT * FROM nba_data where language = 'en' limit 5;")


# PostgreSQL Connection
sparkClassPath = os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.postgresql:postgresql:42.1.1 pyspark-shell'
spark = SparkSession.builder.appName("Practice").config("spark.driver.extraClassPath", sparkClassPath).getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://{os.environ.get('IP')}/{os.environ.get('RDS_DB')}") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", f"{os.environ.get('RDS_SCHEMA')}.aws_odds_source") \
    .option("user", os.environ.get('RDS_USER')) \
    .option("password", os.environ.get('RDS_PW')) \
    .load()

You have to use jdbc and then put in all of your shit.  Note that `dbtable` option actually has to have the schema appeneded to it first (myschema.mytable)

# Reading and Writing Files
parquetDF = spark.read.parquet("/tmp/databricks-df-example.parquet")
parquetDF.write.parquet("databricks-df-example.parquet")

df = spark.read.option('header', 'true').csv('nba_tweets.csv')
df = spark.read.csv('cars.csv', header=True, sep=";")
df = spark.read.csv('cars.csv', header=True, sep=";", inferSchema=True) # use this when loading in dfs

# DataFrame Data Munging
This takes a input df and filters the `color` column to values of `red`, and then it groups by the `owner` column and aggregates the `price` column into a new column called `grouped_price`.
    * To filter on a string you have to use F.lit('str')
    * To aggregate you have to do F.sum('price_column')
    * To do a case when you have to do F.when(F.col('my_col'))

The next dataframe then creates a case when statement for a new column that filters that `grouped_price` column and values of > 10 get populated with 1, while others are just 0 in a variable called `indicator`.  It then filters this column for values of 1.

inter_df = input_df.where(input_df['color'] == \
                        F.lit('red')).groupBy('owner').agg(F.sum('price').alias('grouped_price'))

output_df = inter_df.select('owner', 'grouped_price', \
                            F.when(F.col('grouped_price') > 10, 1).otherwise(0).alias('indicator')).where(
            F.col('indicator') == F.lit(1))

# DataFrame Column Renaming
To rename columns do

df = df.withColumnRenamed('first_column', 'new_column_one') \
       .withColumnRenamed('second_column', 'new_column_two') \
       .withColumnRenamed('third_column', 'new_column_three')


# DataFrame Count / Group by

df.groupBy('Origin').count().show(5)

# DataFrame Column deletion

df = df.drop('new_column_two') \
       .drop('new_column_three')

# DataFrame Filtering

europe_filtered_count = df.filter(col('Origin')=='Europe').count()

df.filter(df.a == 1).show()

# DataFrame distinct rows

df.select('Origin').distinct().show()
df.select('Origin','model').distinct().show()

# Collecting pyspark data
`df.collect()` will put it into memory (i think as a list?)
`df.take(5)` will only take a certain number of records
`df.toPandas()` will turn it back into a Pandas Dataframe

df2 = df.collect()
df2 = df.take(5)
df_pandas = df.toPandas()


# UDFs

from pyspark.sql.functions import pandas_udf

@pandas_udf('long')
def pandas_plus_one(series: pd.Series) -> pd.Series:
    # Simply plus one by using pandas Series.
    return series + 1

df.select(pandas_plus_one(df.a)).show()

# History
Hadoop came out in 2006 which enabled distributing computing: using multiple machines on 1 task by distributing work to them and then collecting and aggregating it together.  This framework to accomplish this task was called MapReduce and it was written in Java.  However, Hadoop was slow and had to write to disk and RAM was not efficiently being used.

Facebook created Hive in 2010 to improve this by using a flavor of SQL called HQL to write MapReduce jobs in a SQL-like language.  However, it still kinda sucked.

Spark was released in 2014 to address the above drawbacks.  It leverages as much RAM as possible which vastly increases compute time which is where the `50-100x faster` statistic comes from.

# Troubleshooting
If you're having OutOfMemory errors - make sure your nodes are utilizing as much RAM as possible and it isn't set to a default number.  A lot of these solutions literally just say "Increase the RAM" like yeet.

```
spark = SparkSession.builder \
    .master('local[*]') \
    .config("spark.driver.memory", "15g") \
    .appName('my-cool-app') \
    .getOrCreate()
```

[Article](https://medium.com/swlh/spark-oom-error-closeup-462c7a01709d)
[Article 2](https://mungingdata.com/apache-spark/broadcast-joins/)

[Video](https://databricks.com/session_na20/on-improving-broadcast-joins-in-apache-spark-sql#:~:text=Broadcast%20join%20is%20an%20important,partitions%20of%20the%20other%20relation.)
Spark splits data up on different nodes so it can do its processing efficiently.  Traditionally, joins are hard because of this.  Broadcast joins are a way of joining 2 relations by first sending the smaller one (broadcasting it) to all nodes in the cluster.  Then it can do the join without going through a shuffle process in the larger DataFrame.

use `.explain()` at the end of your Spark Job to see the explain plan and see what's going on.  Sometimes Spark will automatically be efficient for you, sometimes the code will be too complex and you'll have to intervene and handle things like Broadcast Joins yourself.

There is a `spark.sql.autoBroadcastJoinThreshold` value you can adjust depending on if you run into issues or not.

Think about outliers when running the queries, something about skew and partitions and if the data is skewed then a few of the nodes might get a shit ton of the data and you'll have OOM issues.

# SQL
Do the normal SparkSession entrypoint and then create a tempview with your data, then call `spark.table('tablename')` on the view you just created.  You can then do normal SQL Expression with `spark.sql('select x from tablename')`
```
spark.range(1, 20).createOrReplaceTempView("test")
df = spark.table("test")

spark.sql("select id from test").show()
```