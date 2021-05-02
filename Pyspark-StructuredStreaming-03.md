# Pyspark Structured Streaming - Part 3

## State Management

### Stateful and Stateless

- Spark Structured Streaming offers both stateful and stateless transformation.
- Any agreegation are stateful, most narrow transformation are stateless unless we apply watermark.
- Spark executor stores the state in memory and thus we should be careful about state store size.
- Checkpoint directory is used as a failover mechanism for state store.
- Since stateless jobs output depends only on current batch state information thus `Complete` modes invalid for the same.

### Managed and Unmanaged Stateful Operation

Consider streaming aggregation use case:

Use Case 1: Calculate customer shopping amount in an E-Business application.

```
Transaction 1: 
{'user': 'Sankar', 'shopping': 200, 'date': '20-04-2020'}

Output 1:
{ 'result': [ {'user': 'Sankar', 'total': 200 } ] }

State 1:
{ 'stored': [ {'user': 'Sankar', 'total': 200 } ] }

-----------------------------------------------------------

Transaction 2: 
{'user': 'Jade', 'shopping': 800, 'date': '21-04-2020'}

Output 2:
{ 'result': [ {'user': 'Jade', 'total': 800 } ] }

State 3:
{ 'stored': [ {'user': 'Sankar', 'total': 200 }, {'user': 'Jade', 'total': 800 } ] }

-----------------------------------------------------------

Transaction 3: 
{'user': 'Sankar', 'shopping': 300, 'date': '21-04-2020'}

Output 3:
{ 'result': [ {'user': 'Sankar', 'total': 500 } ] }

State 3:
{ 'stored': [ {'user': 'Sankar', 'total': 500 }, {'user': 'Jade', 'total': 800 } ] }

-----------------------------------------------------------

Transaction 4: 
{'user': 'Kun', 'shopping': 100, 'date': '21-04-2020'}

Output 4:
{ 'result': [ {'user': 'Kun', 'total': 100 } ] }

State 4:
{ 'stored': [ {'user': 'Sankar', 'total': 500 }, {'user': 'Jade', 'total': 800 }, {'user': 'Kun', 'total': 100 } ] }

```

As you can see to give the output of total purchase by user in the application Spark need to maintain the complete history from begining and the state will be infinitely growning and it is quite obvious it can quickly become unmanagable. This is a use case of ***Unmanaged Statefull*** Streaming application and to make this application realistic we need to code custom state management logic for Spark. We will not dig deeep in this kind of application as until now PySpark doesnot support this kind of streaming application.

Now suppose we want ***Weekly*** shopping per user application. This transforms our application to a ***Managed Statefull*** Streaming application as now Spark exactly knows how to manage the state one we let Spark know which feild in input data is for event time.


Use Case 2: Calculate customer weekly shopping amount in an E-Business application.

```
Transaction 1: 
{'user': 'Sankar', 'shopping': 200, 'date': '20-04-2020'}

Output 1:
{ 'result': [ {'user': 'Sankar', 'total': 200 } ] }

State 1:
{ 'stored': [ {'user': 'Sankar', 'total': 200, 'lastshopped': '20-04-2020' } ] }

-----------------------------------------------------------

Transaction 2: 
{'user': 'Jade', 'shopping': 800, 'date': '18-04-2020'}

Output 2:
{ 'result': [ {'user': 'Jade', 'total': 800 } ] }

State 3:
{ 'stored': [ {'user': 'Sankar', 'total': 200, 'lastshopped': '20-04-2020' }, {'user': 'Jade', 'total': 800, 'lastshopped': '18-04-2020' } ] }

-----------------------------------------------------------

Transaction 3: 
{'user': 'Sankar', 'shopping': 300, 'date': '25-04-2020'}

Output 3:
{ 'result': [ {'user': 'Sankar', 'total': 500 } ] }

State 3:
{ 'stored': [ {'user': 'Sankar', 'total': 800, 'lastshopped': '25-04-2020' } ] }

```

***Note*** Spark moves out Jade's data as his history is 7 days old.

```
-----------------------------------------------------------

Transaction 4: 
{'user': 'Kun', 'shopping': 100, 'date': '28-04-2020'}

Output 4:
{ 'result': [ {'user': 'Kun', 'total': 100 } ] }

State 4:
{ 'stored': [ {'user': 'Sankar', 'total': 500, 'lastshopped': '25-04-2020' } }, {'user': 'Kun', 'total': 100, 'lastshopped': '28-04-2020' } } ] }

```

***Note*** Spark moves out Sankar's data of purchase on 20th as it is 7 days old.

As you can see Spark just need to know which field is the `date-time` feild and how long to keep record for (`Window`).

> Note an important consideration here, if you noted clearly Jade's record (Event: 18-04-2020) came after Sankar's first record (Event: 20-04-2020), thus surely the data arrived late (as the actual event obviously occured earlier). This is the next important thing to make Spark aware as such how long you want Spark to wait for late arriving record know as `Watermarking`. 


Since statefull operation require spark to manage a state store which have history of state from previous batches it is critical to manage this state.
Spark gives you to two options statefull operations:

- Managed/Timebound Statefull Operation: State store is only managed based with eventtime and the store is maintained by Spark.
- Unmanaged/Unbounder Statefull Operation: State store is based on custom feild in data and its management need to be definded by user.

Currently ***Pyspark*** only supports ***Managed Statefull*** application, thus we can only work with event time bases statefull application in Pyspark.
In case someone needs custom state one need to switch to Scala or Java.


### Time Bound Aggregations :

First important thing to keep in mind is `Trigger time` has no role to play in event time based aggregation as Spark will not consider it while adding or dropping records form the one considered for aggregation and maintained in state. Tigger time is just the time based on which Spark trigger bactches and nothing to do what's inside the batch.

There are two type of Windows in Timebound aggregation:

- `Tumbling Windows` : Fixed sized time based windows consisting of non overlapping time period. Thus the time ranges are discrete time interval. e.g. 1-5, 5-10, 10-15...
- `Sliding Windows` : Fixed sized continuous overlapping period of time. e.g. 1-5, 2-6, 3-7... 

For the demonstration we consider our first use case:

#### Use Case 1: Continuous Profit Generator. Note the calculation of profit will be very simple the amount of profit is aggregated using buy amount as additive profit and sell amount as negetive profit.

- Data Recieved:

```
{ 'id': 'apple', 'amount': 10, 'type': 'buy', 'time': '20-04-2020-10-10' },
{ 'id': 'ibm', 'amount': 15, 'type': 'buy', 'time': '20-04-2020-10-16' },
{ 'id': 'dell', 'amount': 8, 'type': 'sell', 'time': '20-04-2020-10-17' },
{ 'id': 'apple', 'amount': 5, 'type': 'sell', 'time': '20-04-2020-10-22' },
{ 'id': 'shell', 'amount': 18, 'type': 'buy', 'time': '20-04-2020-10-12' },
{ 'id': 'asus', 'amount': 5, 'type': 'buy', 'time': '20-04-2020-10-25' },
{ 'id': 'goggle', 'amount': 30, 'type': 'buy', 'time': '20-04-2020-10-27' }
```

- The code for data collection is standard and we should be familier with it by now.

```
    input_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "spark-stream-05") \
        .option("startingOffsets", "earliest") \
        .load()

    schema1 = StructType(
        [   StructField('id', StringType()),
            StructField('type', StringType()),
            StructField('amount', IntegerType()),
            StructField('time', StringType())
        ])

    transformed_df_withoutdate = input_df.select(col("value").cast("string")) \
        .withColumn("value_json", from_json(col("value"), schema1)) \
        .select("value_json.*")
        
    # To calculate profit we make selling transaction record as negative.
    # This will help us calculate profit but just aggregating this field.
    transformed_df = transformed_df_withoutdate.withColumn(
        'eventtime',
        to_timestamp( col('time'), 'dd-MM-yyyy-HH-mm' )
    ).drop('time').withColumn(
        'profit',
        when( col('type') == 'sell', ( -1 * col('amount') ) ).otherwise(
            col('amount')
        )
    )
    
```

> Note we have transformed the String field time to a Timestamp typed event time column as this will be used by Spark for windowing.
> Also we have created a profit field to apply simple aggregation of profit, for sell transaction we make profit as negetive and for but we make positive profit.

- Now the schema of the transformed data frame is as below :

```
root
 |-- id: string (nullable = true)
 |-- type: string (nullable = true)
 |-- amount: integer (nullable = true)
 |-- eventtime: timestamp (nullable = true)
 |-- profit: integer (nullable = true)
```

- Now we start to calculate the profit per 5 minutes by appling a window aggregation. Note here the windowing can be on only a date/time field. Also note we could have added additional fields to the windowing function thus creating fine grain grouping.

```
    agg_df = transformed_df.groupby(
        window(
            'eventtime', '5 minute'
        )
    ).agg( sum('profit').alias('profit') )
```

- To get profit per ID per 5 minutes we could have done below:

```
    agg_df = transformed_df.groupby(
        window(
            'eventtime', '5 minute', col('id')
        )
    ).agg( sum('profit').alias('profit') )
```

- Its time to write the result, for simplicity we write the complete result to console for simplicity and easy visualization.

```
    stream_query = agg_df.writeStream.format("console") \
        .option("checkpointLocation", "checkpoint") \
        .outputMode("complete") \
        .option("truncate", "false") \
        .start()

    stream_query.awaitTermination()
```

- Lets create topic and push our first record while our application is running.

```
(venv) apple@apples-MacBook-Air PysparkStreaming % ../../Softwares/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic spark-stream-05 --partitions 1 --replication-factor 1
Created topic spark-stream-05.
(venv) apple@apples-MacBook-Air PysparkStreaming % ../../Softwares/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic spark-stream-05
>{ 'id': 'apple', 'amount': 10, 'type': 'buy', 'time': '20-04-2020-10-10' }

```

Result expected.

```
-------------------------------------------
Batch: 1
-------------------------------------------
+------------------------------------------+------+
|window                                    |profit|
+------------------------------------------+------+
|[2020-04-20 10:10:00, 2020-04-20 10:15:00]|10    |
+------------------------------------------+------+
```

> Note since we have no trigger time Spark immediatly trigger batch when event arrive. 
> Since the record is of timestamp *20-04-2020-10-10* and out window if of 5 minutes he puts this in an window of *2020-04-20 10:10:00, 2020-04-20 10:15:00* and calculates profit.
> Note the trigger time when the batch was triggered has nothing to do with this windowing operation, meaning the time when this batch was triggered could be any valid time in history or future.

Now lets generate two more events.

```
>{ 'id': 'shell', 'amount': 18, 'type': 'buy', 'time': '20-04-2020-10-12' }
>{ 'id': 'apple', 'amount': 5, 'type': 'sell', 'time': '20-04-2020-10-22' } 
```

Expected result

```
-------------------------------------------
Batch: 2
-------------------------------------------
+------------------------------------------+------+
|window                                    |profit|
+------------------------------------------+------+
|[2020-04-20 10:10:00, 2020-04-20 10:15:00]|28    |
+------------------------------------------+------+

-------------------------------------------
Batch: 3
-------------------------------------------
+------------------------------------------+------+
|window                                    |profit|
+------------------------------------------+------+
|[2020-04-20 10:10:00, 2020-04-20 10:15:00]|28    |
|[2020-04-20 10:20:00, 2020-04-20 10:25:00]|-5    |
+------------------------------------------+------+
```

> Note every time an event arrive Spark triggers a batch as we have not given an trigger time.
> The first event sent this time is of time *20-04-2020-10-12* for which Spark already had a window and profit calculated this he aggregated new result with history.
> For second one *20-04-2020-10-22* he needed a new window and thus he created one. Note here *Tumbling* window doesnot mean the windows that Spark create has to be consicutive, he will create window only if it has event to fit into.


Now let push a late arriving event.

```
>{ 'id': 'asus', 'amount': 5, 'type': 'buy', 'time': '20-04-2020-10-14' }

```

Expected result

```
-------------------------------------------
Batch: 4
-------------------------------------------
+------------------------------------------+------+
|window                                    |profit|
+------------------------------------------+------+
|[2020-04-20 10:10:00, 2020-04-20 10:15:00]|33    |
|[2020-04-20 10:20:00, 2020-04-20 10:25:00]|-5    |
+------------------------------------------+------+
```

> Note how Spark looks up his state store finds and already available window to fit the new event into and calculates the new aggregate and publish results.


