# Trino
[Guide](https://github.com/trinodb/trino/tree/master/core/docker)

## How It Works
Trino is a distributed SQL query engine designed for querying and analyzing large volumes of data across multiple data sources. It provides a unified interface for querying data stored in various sources, such as data lakes, relational databases, and more. Trino's basic layout consists of several key components:

1. Coordinator Node:
   - The Coordinator Node is responsible for managing query execution. When a query is submitted to Trino, the Coordinator Node receives the query, plans its execution, and coordinates with other nodes to execute the query efficiently.
   - It maintains metadata about the system, including information about the available worker nodes and data sources.

2. Worker Nodes:
   - Worker Nodes are responsible for executing the actual tasks associated with a query. These tasks include scanning data, filtering, aggregating, and more.
   - Trino can have multiple Worker Nodes that can be horizontally scaled to handle large workloads.

3. Query Execution:
   - When a query is submitted, the Coordinator Node parses it, optimizes the execution plan, and distributes the tasks across the Worker Nodes.
   - Trino's query engine is designed for parallel and distributed processing, allowing it to execute queries quickly, especially when dealing with large datasets.

4. Connectors:
   - Trino supports a variety of data connectors, also known as connectors or plugins, which allow it to access data stored in various sources. Some common connectors include connectors for HDFS (Hadoop Distributed File System), Hive, MySQL, PostgreSQL, and many others.
   - Connectors are responsible for translating SQL queries into source-specific operations and fetching the data from the underlying data source.

5. Storage Systems:
   - Trino can query data stored in a wide range of storage systems, including data lakes (e.g., HDFS and S3), relational databases (e.g., MySQL, PostgreSQL), NoSQL databases, and more.
   - Trino's architecture allows it to query data across multiple data sources simultaneously, making it suitable for federated queries.

6. SQL Interface:
   - Trino provides a SQL-like query language that allows users to interact with and query the data stored in various sources using familiar SQL syntax. This SQL interface simplifies data analysis and retrieval.

7. Security:
   - Trino offers security features such as authentication and authorization, allowing organizations to control who can access and query their data.

8. Extensibility:
   - Trino is highly extensible and can be customized using connectors and functions to meet specific requirements and integrate with various data sources.

In summary, the basic layout of Trino includes a Coordinator Node that manages query execution and multiple Worker Nodes responsible for performing the actual query tasks. It connects to various data sources via connectors and provides a SQL interface for querying and analyzing data across these sources. Trino's distributed and parallel processing capabilities make it a powerful tool for big data analytics and querying data from diverse data stores.

### Glue Catalog
[Link](https://trino.io/docs/current/connector/metastores.html#aws-glue-catalog-configuration-properties)
[Link2](https://tabular.io/blog/docker-spark-and-iceberg/)
[Link3](https://github.com/myfjdthink/starrocks-iceberg-docker)

Dont fucking exec and run the trino CLI in a terminal lmfao, get a dedicated db management tool like datagrip or dbeaver
``` sh
docker exec -it trino-trino-1 trino


```


### Apache Iceberg
I guess they separate out the Glue & S3 Credentials incase you have 1 or the other in different accounts?

`etc/catalog/iceberg.properties`
```
connector.name=iceberg
iceberg.file-format=parquets
iceberg.catalog.type=glue
iceberg.file-format=parquet
hive.metastore.glue.region=us-east-1
hive.metastore.glue.aws-access-key=zzz
hive.metastore.glue.aws-secret-key=aaa
hive.metastore.glue.default-warehouse-dir=s3://jyablonski2-iceberg/
hive.metastore.glue.catalogid=717791819289
hive.s3.aws-access-key=zzz
hive.s3.aws-secret-key=aaa
```

