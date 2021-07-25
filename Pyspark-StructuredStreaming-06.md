# Pyspark Structured Streaming - Part 6

## Joins - Streaming to Static Dataframe

The best thing about Structured Streaming is it brings streaming applications as close to static dataframe handling, this is no difference with `Joins`. Its so similar to normal dataframe join that there is nothing much to share, thus lets see an example.


## Stream to Static Inner Join are Stateless

Let us understand the meaning, note first whats the implication of this statement. Once a action becomes Statefull, Spark has to manage statestore, why ?
Because when Spark output result of batch action, it cannot confirm that it is done for the dataset for the batch, because there might be late arrival on way. Now we have seen before to remediate we use `watermarking` to keep Statestore managable.

With Stream to Static Inner Join since one side of the join is always known Spark doesnot need to manage any state.


### Use Case: Lets try to update `last login` detail of an employee in a table in Cassandra with streaming `login` data comming to Kafka


> Note the choice of Cassandra is significant as it allow insert by key as an ***UPSERT*** operation, other similar choice would have been Apache HBase. 
> Upsert means that Cassandra will insert a row if a primary key does not exist already otherwise if primary key already exists, it will update that row.


Application will perform the requirement with following operations:

- Read Cassandra table for employee data (static dataframe load).
- Read Kafka topic for incomming login record (streaming dataframe load).
- Join two dataframe with `Inner` join. The `updated` rows of the joined dataframe will give us employee who has new login record streamed in to Kafka.
- Project joined dataframe with employee table columns just renaming the login column from streaming dataframe as new `last login` to be writen to Cassandra.
- Save dataframe to Cassandra.


### Implementation


- First lets add Spark session configuration to load [Cassandra connector](https://github.com/datastax/spark-cassandra-connector).

```
    spark = SparkSession \
        .builder \
        .appName("Kafka Cassandra Connector") \
        .config("spark.sql.shuffle.partitions", 2) \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.executor.extraClassPath", "/Users/apple/PycharmProjects/jars/*") \
        .config("spark.driver.extraClassPath", "/Users/apple/PycharmProjects/jars/*") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0") \
        .config("spark.cassandra.connection.host", "127.0.0.1") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()
```


- Next lets load static data from Cassandra

```
    sta_inp_df = spark.read\
        .format("org.apache.spark.sql.cassandra")\
        .option("keyspace", "sankar")\
        .option("table", "emp")\
        .load()
```


- Next lead read streaming data from Kafka and transform in desired format

```
    str_inp_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "spark-stream-07") \
        .option("startingOffsets", "earliest") \
        .load()

    schema1 = StructType([StructField('id', StringType()),
                        StructField('login', StringType())
                        ])

    str_trans_df = str_inp_df.select(col("value").cast("string")) \
        .withColumn("value_json", from_json(col("value"), schema1)) \
        .select("value_json.*") \
        .withColumn( 'login', to_timestamp(col('login'), 'dd-MM-yyyy-HH-mm') )
```


- At this point we have static and streaming dataframe, lets join and transform to our desired output format to synchronize back to Cassandra.

```
    j_expr = str_trans_df.id == sta_inp_df.id
    j_type = "inner"
    out_df = str_trans_df.join(sta_inp_df, j_expr, j_type).drop(str_trans_df.id)\
        .select(col("id"),
                col("name"),
                col("login").alias("last_login"))
```


- Now sinse Spark currently does not have a Cassandra `sink`, thus we use the generic `foreachBatch` to get our job done.

```
    stream_query = out_df.writeStream\
        .foreachBatch(write_to_cassandra)\
        .option("checkpointLocation", "checkpoint") \
        .outputMode("update") \
        .trigger(processingTime="1 minute") \
        .start()

    stream_query.awaitTermination()
```


- Finally we implement the `write_to_cassandra` function.

```
def write_to_cassandra(target_df, batch_id):
    target_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "sankar") \
        .option("table", "emp") \
        .mode('append') \
        .save()

    target_df.show()
```

> Note we write to Cassandra in append mode (as it supports Upsert) and we also sync it to console for validation.


### Result

- Set up Cassandra. You will find a sample article [here](https://mukherjeesankar.wordpress.com/2021/07/24/setup-local-cassandra-for-testing/). After setup if you view emp table in Cassandra you should see below output:

```
apple@apples-MacBook-Air bin % ./cqlsh localhost 9042
Connected to Test Cluster at localhost:9042.
[cqlsh 5.0.1 | Cassandra 4.0-beta2 | CQL spec 3.4.5 | Native protocol v4]
Use HELP for help.
cqlsh> use sankar ;
cqlsh:sankar> select * from emp ;

 id | last_login                      | name
----+---------------------------------+------
  3 | 2021-02-10 11:20:00.000000+0000 |  ram
  2 | 2021-02-10 14:20:00.000000+0000 |  sam
  1 | 2021-02-11 11:10:50.000000+0000 | jodu

(3 rows)
```


- Now if we push messages to Kafka topic as below

```
(venv) apple@apples-MacBook-Air PysparkStreaming % ../../Softwares/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic spark-stream-07
>{ 'id': 'jodu', 'login': '06-03-2021-10-24' }
>{ 'id': '1', 'login': '06-03-2021-10-24' }
>{ 'id': '3', 'login': '06-03-2021-11-05' }
>
```

> Note, the first record will not join with any employee record thus we will see no join output.


- Console output will be as below:

```
+---+----+----------+
| id|name|last_login|
+---+----+----------+
+---+----+----------+

+---+----+-------------------+
| id|name|         last_login|
+---+----+-------------------+
|  1|jodu|2021-03-06 10:24:00|
+---+----+-------------------+

+---+----+-------------------+
| id|name|         last_login|
+---+----+-------------------+
|  3| ram|2021-03-06 11:05:00|
+---+----+-------------------+
```


- Now if we check Cassandra we will see two rows updated.

```
apple@apples-MacBook-Air bin % ./cqlsh localhost 9042
Connected to Test Cluster at localhost:9042.
[cqlsh 5.0.1 | Cassandra 4.0-beta2 | CQL spec 3.4.5 | Native protocol v4]
Use HELP for help.
cqlsh> use sankar ;
cqlsh:sankar> select * from emp ;

 id | last_login                      | name
----+---------------------------------+------
  3 | 2021-03-06 05:35:00.000000+0000 |  ram
  2 | 2021-02-10 14:20:00.000000+0000 |  sam
  1 | 2021-03-06 04:54:00.000000+0000 | jodu

(3 rows)
cqlsh:sankar> quit
```


