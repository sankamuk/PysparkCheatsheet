# Pyspark Structured Streaming - Part 7

## Join - Sream to Stream

- Firstly we need to understand joining two stream even if it is `Inner` join is a `Statefull` operation, beacuse on both side of join Spark knows is a partial dataset. 

Implication of this is Spark maintain both side in in Statestore, thus you have to be extra careful with good cleanup strategy (with `watermark`).

- Secondly what mode you choose, complete or update or append.

Understand there is nothing to update in a join, either you join or dont join thus `append` mode should be the mode to choose.

- Duplicate record need to carefully thought about as it may affect your result's consistency.

Spark does not uderstand the implication of the duplicate record and its result on data consistency and it need to though and issue need to handled in design.

- Stream to Stream join can be `one to one`, `one to many` or `many to many`. 


### Use Case: Host to Logged-in user match

Let consider we have a cloud environment were user login perform action and logout after some time host gets destroyed.

Thus we have stream record for host as below:
```
{ 'id': '22', 'type': 'linux', 'service': 'mysql', 'start_time': '06-03-2021-10-10' }
{ 'id': '34', 'type': 'linux', 'service': 'postgre', 'start_time': '06-03-2021-10-25' }
{ 'id': '44', 'type': 'windows', 'service': 'sqlserver', 'start_time': '06-03-2021-10-33' }
```

Also we have stream record for user login:
```
{ 'id': '1', 'user': 'ram', 'login': '06-03-2021-10-15', 'host_id': '22' }
{ 'id': '12', 'user': 'sam', 'login': '06-03-2021-10-24', 'host_id': '22' }
{ 'id': '1', 'user': 'ram', 'login': '06-03-2021-10-30', 'host_id': '34' }
{ 'id': '12', 'user': 'sam', 'login': '06-03-2021-10-40', 'host_id': '22' }
{ 'id': '8', 'user': 'jodu', 'login': '06-03-2021-10-50', 'host_id': '44' }
```

Now we need to join these two stream to do further analysis, but here we just do console output.

### Implementation

First we get out two stream created from Kafka source.

```
    str_inp_01 = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "spark-stream-001") \
        .option("startingOffsets", "earliest") \
        .load()

    str_inp_02 = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "spark-stream-002") \
        .option("startingOffsets", "earliest") \
        .load()
```

Next lets parse data with Schema and then transform it as per requirement. Then we set `watermark` interval to avoid massive state store management.

```
    schema01 = StructType(
        [   StructField('id', StringType()),
            StructField('type', StringType()),
            StructField('service', StringType()),
            StructField('start_time', StringType())
        ])

    schema02 = StructType(
        [   StructField('id', StringType()),
            StructField('user', StringType()),
            StructField('login', StringType()),
            StructField('host_id', StringType())
        ])

    trans_df_01 = str_inp_01.select(col("value").cast("string")) \
        .withColumn("value_json", from_json(col("value"), schema01)) \
        .select("value_json.*").withColumn( 'start_time', to_timestamp( col('start_time'), 'dd-MM-yyyy-HH-mm' ))\
        .withWatermark('start_time', '20 minute')

    trans_df_02 = str_inp_01.select(col("value").cast("string")) \
        .withColumn("value_json", from_json(col("value"), schema02)) \
        .select("value_json.*").withColumn( 'login', to_timestamp( col('login'), 'dd-MM-yyyy-HH-mm' ))\
        .withWatermark('login', '20 minute')
```

With this we are ready to join as usual, nothing special here.

```
    j_expr = trans_df_01.id == trans_df_02.host_id
    j_type = 'inner'
    join_df = trans_df_01.join(trans_df_02, j_expr, j_type)
```

Finally we push result to sink.

```
    stream_query = join_df.writeStream.format("console") \
        .option("checkpointLocation", "checkpoint") \
        .outputMode("append") \
        .option("truncate", "false") \
        .trigger(processingTime="1 minute") \
        .start()
```

> Note output mode is append.

#### Result

If all goes well we should get what we expected.

```
```




