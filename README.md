# PySpark Prac
Practice Repo for PySpark Goodness.

## Streaming Example
An Example Streaming Application is in the `streaming/` folder.  You can run `make streaming-up` which will spin up Containers for the following resources:
- Zookeeper
- Kafka
- Kafka UI
- Spark
- Cassandra
- Python Data Producer

The Data Producer writes fake dummy data to a Topic called `jacobs-topic` in Kafka.  The Kafka UI Image will spin up a Web Page to display the Kafka Broker & all topics at `http://localhost:8080/`.

2 PySpark Scripts are in the folder that are meant to be ran locally; these will connect to Kafka and stream the topic data to Cassandra & Console output respectively.

When finished run `make streaming-down` to spin the resources down.

## Trino Example
Run `make trino-up` to spin up a Trino Database

When finished run `make trino-down` to spin the resources down.
