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

# Then, run the command to create 'odometry' topic in 'ros'
create table jyablonski.jacobs_topic(
        id varchar primary key, 
        name varchar,
        email varchar,
        price float,
        dividend int,
        scrape_ts timestamp
    );

# Check your setup is correct
DESCRIBE jyablonski.jacobs_topic

describe keyspace jyablonski;

select * from jyablonski.jacobs_topic;
select count(*) from jyablonski.jacobs_topic;
```

![image](https://github.com/jyablonski/pyspark_prac/assets/16946556/d8140020-8e3e-4b52-ae6f-a67efb6a1802)
