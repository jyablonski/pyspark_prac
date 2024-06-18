# Spark Streaming
`kafka_to_cassandra.py` will run a PySpark Script to read from Kafka and stream messages to Cassandra
`kafka_to_console.py` will run a PySpark Script to read from Kafka and stream messages to Console Output

``` sh
# Execute cassandra container with container id given above
docker exec -it 1c31511ce206 bash

# Open the cqlsh
cqlsh -u cassandra -p cassandra

# Run the command to create 'ros' keyspace
CREATE KEYSPACE jyablonski WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

# use this for the console write script
create table jyablonski.jacobs_topic(
        id varchar primary key, 
        name varchar,
        email varchar,
        price float,
        dividend int,
        scrape_ts timestamp
    );

# use this for cassandra write script - has an extra transformation column
create table jyablonski.jacobs_topic(
        id varchar primary key, 
        name varchar,
        email varchar,
        price float,
        dividend int,
        scrape_ts timestamp,
        spark_ts timestamp
    );

# Check your setup is correct
DESCRIBE jyablonski.jacobs_topic

describe keyspace jyablonski;

select * from jyablonski.jacobs_topic limit 10;

select max(scrape_ts) from jyablonski.jacobs_topic limit 10;
select count(*) from jyablonski.jacobs_topic;
```

![image](https://github.com/jyablonski/pyspark_prac/assets/16946556/d8140020-8e3e-4b52-ae6f-a67efb6a1802)


## Streaming

[Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#triggers)

The “Output” is defined as what gets written out to the external storage. The output can be defined in a different mode:

- Complete Mode - The entire updated Result Table will be written to the external storage. It is up to the storage connector to decide how to handle writing of the entire table.

- Append Mode - Only the new rows appended in the Result Table since the last trigger will be written to the external storage. This is applicable only on the queries where existing rows in the Result Table are not expected to change.

- Update Mode - Only the rows that were updated in the Result Table since the last trigger will be written to the external storage (available since Spark 2.1.1). Note that this is different from the Complete Mode in that this mode only outputs the rows that have changed since the last trigger. If the query doesn’t contain aggregations, it will be equivalent to Append mode.

The `foreach` and `foreachBatch` operations allow you to apply arbitrary operations and writing logic on the output of a streaming query. They have slightly different use cases - while foreach allows custom write logic on every row, foreachBatch allows arbitrary operations and custom logic on the output of each micro-batch.

`foreachBatch` allows you to specify a function that is executed on the output data of every micro-batch of a streaming query. Since Spark 2.4, this is supported in Scala, Java and Python. It takes two parameters: a DataFrame or Dataset that has the output data of a micro-batch and the unique ID of the micro-batch.

![image](https://github.com/jyablonski/pyspark_prac/assets/16946556/f0a04d85-f0d9-42f2-9959-3e3564e5c5a9)


``` py


query = df.writeStream.format("console").start()   # get the query object

query.id()          # get the unique identifier of the running query that persists across restarts from checkpoint data

query.runId()       # get the unique id of this run of the query, which will be generated at every start/restart

query.name()        # get the name of the auto-generated or user-specified name

query.explain()   # print detailed explanations of the query

query.stop()      # stop the query

query.awaitTermination()   # block until query is terminated, with stop() or with error

query.exception()       # the exception if the query has been terminated with error

query.recentProgress  # a list of the most recent progress updates for this query

query.lastProgress    # the most recent progress update of this streaming query


```