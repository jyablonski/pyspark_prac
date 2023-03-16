# Data Lake

## Apache Iceberg
[Article](https://alexmercedcoder.medium.com/configuring-apache-spark-for-apache-iceberg-74ad039cdb6e)
[Article 2](https://github.com/apache/iceberg/issues/3546)
[Iceberg Official Notes](https://iceberg.apache.org/docs/latest/aws/)


DynamoDB can be used to perform table locking in S3 to enable ACID Transactions.

AWS Glue Catalog can be used to store table information in Glue Tables under a Glue Database.  
- Anytime a schema change occurs, A Glue TableVersion is created. 
- Tables have a Version ID attached to them in their metadata.  When they're updated, this ID is incrementally increased.  This is used as a table locking feature.  
- Users can update the table as long as this ID has not been changed on the server side.  If it's different, the update fails and you'll have to refresh the metadata to retrieve the most up-to-date version.

Warehouse is a term used to refer to the root location in S3.  Looks like `s3://jyablonski-iceberg/my_db/` and a table in it would look like `s3://jyablonski-iceberg/my_db/table1`.

Create Iceberg tables using the `USING iceberg` syntax in a `CREATE TABLE` statement.
```
CREATE TABLE my_catalog.my_ns.my_table (
    id bigint,
    data string,
    category string)
USING iceberg
OPTIONS ('location'='s3://my-special-table-bucket')
PARTITIONED BY (category);
```

Time Travel features are enabled via turning on File Versioning in the Data Lake.

# Spark Config

There can only be 1 `spark.jars.packages` in a Spark Config.  If there are more they will just overwrite each other.

```
from pyspark.sql import SparkSession

spark_packages = [
    'org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.1.0',
    'software.amazon.awssdk:bundle:2.16.43',
    'software.amazon.awssdk:url-connection-client:2.16.43',
    'org.apache.hadoop:hadoop-aws:3.3.1',
]


spark = SparkSession \
    .builder \
    .config('spark.jars.packages', ','.join(spark_packages)) \
```