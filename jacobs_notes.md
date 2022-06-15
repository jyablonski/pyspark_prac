# Spark Entrypoint
`spark = SparkSession.builder.appName("Practice").getOrCreate()` is the entrypoint into Spark.  Before version 2.x.x, it was SparkContext or SQLContext or Hivecontext, but now SparkSession inherits all 3 for backwards compatibility.  You should only ever use `SparkSession`.

SparkContext establishes communication with the cluster and resource managers to coordinate and execute jobs on the worker nodes that are either remote or that get spun up locally on your computer.  Need to set an app name and then use getOrCreate().

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
Python UDFs in Spark are considerably slower than ones written in Scala.  Data Types in Python don't match 1:1 to Java, so when using these UDFs the data has to be serialized from Java, read into a separate Python process in order to do the calculation, and the result has to be serialized/deserialized again before returning to the JVM and gathering the result.  So there is a performance hit.

```
from pyspark.sql.functions import pandas_udf

@pandas_udf('long')
def pandas_plus_one(series: pd.Series) -> pd.Series:
    # Simply plus one by using pandas Series.
    return series + 1

df.select(pandas_plus_one(df.a)).show()
```

# History
Hadoop came out in 2006 which enabled distributing computing: using multiple machines on 1 task by distributing work to them and then collecting and aggregating it together.  This framework to accomplish this task was called MapReduce and it was written in Java.  However, Hadoop was slow and had to write to disk and RAM was not efficiently being used.  Some specific software components like YARN were also built that everybody started using and wanted to keep as new tooling was built, hence a lot of big data frameworks are written in Java and use these components.  

These frameworks sit on top of the JVM which is/was relatively easier to build these tools out of than C or C++ because memory management there is more difficult.  However, C++ is faster and some of these tools are rebuilt there for speed purposes.

Facebook created Hive in 2010 to improve this by using a flavor of SQL called HQL to write MapReduce jobs in a SQL-like language.  However, it still kinda sucked.

Spark was released in 2014 to address the above drawbacks.  It's written in Scala and leverages as much RAM as possible which vastly increases compute time which is where the `50-100x faster` statistic comes from.

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

Adding packages or JARs (java archive files) to PySpark can be done in multiple ways.  Setting ENV Variables, adding to $SPARK_HOME/conf, etc.
    * Easiest way seems to be adding config variables when you create your Spark Session.
    * `spark = SparkSession.builder.appName('my_awesome')\
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1')\
    .getOrCreate()`
    * When you create the Spark Session it will download any specific addons you specify and use them in your Spark Job.
      * Make sure versions match here - Spark Version 3.2.1 so use the package version 3.2.1.
    * JAR files is a compressed file containing Classes and other code for a specific function.
      * When working with Postgres or AWS S3 or other specific things, you need to install the JAR for them.


# API Types
## DataFrames
The Api that is most typically used.  Attempts to overcome the limitations of RDDs and provide a modern data structure.  Can read & write data from formats like CSV, JSON, AVRO, HDFS, and Hive.  Data is organized into named columns, aka relational data like in SQL or a dataframe in Pandas / R.

Catalyst optimizer is used for optimization purposes.

## RDD
Outdated, don't use.  Fault tolerant data structure that can store partitioned data across multiple nodes in a cluster and allows parallel processing.

## Dataset
Not available in PySpark, only Java and Scala.  Brings benefits from both RDD and DataFrames.  Type-safety / strong typing, lambda functions and also the benefits of the Spark SQL execution engine.

You can convert RDD and DataFrames to the other's type, and DataFrames & Datasets to RDDs.

## Task
Single operation applied to a single partition.  Each task is executed on a single thread in an executor.  2 Partitions will trigger 2 tasks.  An executor with 16 cores can have 16 tasks each working on their own partition in parallel.

## Stage
A sequence of tasks that can all be ran together, in parallel, without a shuffle.  Others might need to be ran sequentially.

## Job
A sequence of stages, triggered by actions like `count()`, `collect()`, or `write()`.

## Plans
[Article](https://medium.com/the-code-shelf/spark-query-plans-for-dummies-6f3733bab146)
Spark's optimization engine, the Catalyst optimizer, generates the Logical and physical plans.

Logical plan verifies if the operation is correct, identifies data types, location of the data / columns, and validates the operation.

A physical plan is then created where spark decides which algorithm to use for each operator.  Which join (SortMergeJoin or BroadcastHashJoin), is used.

Finally the best physical plan is chosen.

Data gets pulled in, filters are applied, only the necessary columns are carried through the task, shuffles/exchanges occur to move data around during joins, shuffles, and repartitions.

# How Spark Works
[Pic Link](https://cdn.discordapp.com/attachments/272962334648041474/974674566179868732/unknown.png)
[Spark Docs](https://www.dcc.fc.up.pt/~edrdo/aulas/bdcc/classes/spark_arch/bdcc_spark_arch.html)

Spark processes everything in the JVM.  When you use something like PySpark you are basically just writing Python code that is wrapping the actual function calls that happen in Java/Scala.

The entrypoint into Spark is the SparkSession call.  This starts the cluster manager and tells it that there's work that will need to be executed.  It sends your application code written in Python/Scala etc. to the executors as well as the tasks to them so they can be executed.

The Cluster Manager
    * Standalone - entire application runs on 1 machine
    * YARN - resource manager in Hadoop 2
    * Mesos - deprecated
    * Kubernetes - runs things in containers
  
      * As far as I'm aware, this cluster manager can manage MULTIPLE Spark Applications and allocate resources to different Spark Projects that are connected to it.
      * Different applications running on different SparkSession contexts run in their own JVM, and cannot share data unless communicating with an external storage system.
      * Cluster will tell you how much RAM you're using and how much you have available, how many worker nodes, how many applications, and the RAM/CPU of each worker node.
      * There is only one cluster manager (master node) per Cluster.
  
The Cluster Manager communicates with worker nodes that each have their own cache, CPU, and memory to complete tasks and store data.
    * The type of cluster manager doesn't really matter, the underlying architecture should be relatively the same.
    * Worker nodes host the executors responsible for the actual execution of tasks.

The Driver Program is your local computer or remote server you're working on where your code sits and where you start the Spark entrypoint.  This directly launches your spark tasks on the worker nodes, which makes sense; when we call `collect().` on data we're doing that in the driver program and the worker node executes that task for us.
    * Each application only have 1 Driver Program.
    * It coordinates the Spark program
    * It contains the SparkContext object.
    * It's responsible for scheduling the execution of data by worker nodes when in cluster mode.
    * Should be close as possible to worker nodes for optimal performance.

Spark doesn't schedule any processing until you tell it to with calls like `.collect()`. or `.write()`, it will instead just store the instructions.

Spark runs are called jobs, and jobs run in stages.  Stages for the same job can not run in parallel, you must wait for the previous stage to finish.
    * Shuffle Operations are when you repartition your data across nodes, ideally evenly distributing it so you don't run into skew issues or nodes that have 90% of the data and others have 10% of the data.
    * A Shuffle is the process by which data is compared across partitions.

Like SQL, Spark programs have an execution plan that you can view to see what's going on under the hood.
    * Logical execution plans are structured in terms of dataframe transformations and are independent of the cluster characteristics.
    * Phyiscal execution plans are compiled from the logical plan and define the actual job stages and their component tasks, and may include cluster characteristics.

Narrow transformations just map input data to 1 partition.  Wide Transformations may read from multiple different partitions, do some processing logic, and then store to many output partitions.  This requires the reshuffling of data across executors.

In cluster mode, a spark Application Driver and its executors all run inside the cluster in assocation with its workers.

In client mode, the application driver runs in a machine outside of the cluster.  This is more flexible, better security reasons (user code may not be trustworthy) etc.
