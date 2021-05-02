
# Pyspark Structured Streaming - Part 4

## Watermarking - Cleanup Timebound Aggregation State Store

Until know we seen how Spark manages state store to give us a time bound window aggregation streaming application. But as we pointed out before this is an infinitly growing store and qickly will become un manggable.
This state store is managed in all executor and driver and if not managed will bring down the whole process with OuOfMemory and other issues. 
Thus its important to let Spark know how to clean this state store up with not needed data (note this will depend on busness requirements), and we do this with ***watermarking***.

With watermarking we let Spark know considering the latest windows how much old window data is irrelevent and data for the same can be dropped from state store.

Thus with watermarking enabled the current active windows is calculated as:

Active Window End Time: MAX( Event Time )
Active Window Start Time: MAX( Event Time ) - Watermark Value

Thus if latest event recieved at `2020-04-20 10:25:00` and Watermark is `30 minutes` then any Window whose end time is earlier than  `2020-04-20 9:55:00` will be dropped.
This window `2020-04-20 9:30:00, 2020-04-20 9:45:00` and earlier will be dropped.

### Use Case 2: Lets update our previous example with water marking. 

The only update in the code is to add a water mark constrain just before grouping syntax.

```
    agg_df = transformed_df\
    .withWatermark('eventtime', '15 minute')\
    .groupby(
        window(
            'eventtime', '5 minute'
        )
    ).agg( sum('profit').alias('profit') )
```

> Note the watermark directive should come before grouping.
> Watermark should be done on the even time field on which group by will be performed.

Now lets produce events.

```
(venv) apple@apples-MacBook-Air PysparkStreaming % ../../Softwares/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic spark-stream-05
>{ 'id': 'apple', 'amount': 10, 'type': 'buy', 'time': '20-04-2020-10-10' }
>{ 'id': 'apple', 'amount': 5, 'type': 'sell', 'time': '20-04-2020-10-22' }
>{ 'id': 'dell', 'amount': 8, 'type': 'sell', 'time': '20-04-2020-10-40' }
>{ 'id': 'shell', 'amount': 18, 'type': 'buy', 'time': '20-04-2020-10-02' }

```

Lets see what we get as an output.

```
-------------------------------------------
Batch: 1
-------------------------------------------
+------------------------------------------+------+
|window                                    |profit|
+------------------------------------------+------+
|[2020-04-20 10:10:00, 2020-04-20 10:15:00]|10    |
+------------------------------------------+------+

-------------------------------------------
Batch: 2
-------------------------------------------
+------------------------------------------+------+
|window                                    |profit|
+------------------------------------------+------+
|[2020-04-20 10:10:00, 2020-04-20 10:15:00]|10    |
|[2020-04-20 10:20:00, 2020-04-20 10:25:00]|-5    |
+------------------------------------------+------+

-------------------------------------------
Batch: 3
-------------------------------------------
+------------------------------------------+------+
|window                                    |profit|
+------------------------------------------+------+
|[2020-04-20 10:10:00, 2020-04-20 10:15:00]|10    |
|[2020-04-20 10:20:00, 2020-04-20 10:25:00]|-5    |
|[2020-04-20 10:40:00, 2020-04-20 10:45:00]|-8    |
+------------------------------------------+------+

-------------------------------------------
Batch: 4
-------------------------------------------
+------------------------------------------+------+
|window                                    |profit|
+------------------------------------------+------+
|[2020-04-20 10:10:00, 2020-04-20 10:15:00]|10    |
|[2020-04-20 10:20:00, 2020-04-20 10:25:00]|-5    |
|[2020-04-20 10:00:00, 2020-04-20 10:05:00]|18    |
|[2020-04-20 10:40:00, 2020-04-20 10:45:00]|-8    |
+------------------------------------------+------+
```

***Error ?***

- Why event `20-04-2020-10-02` still was considered when i had set watermark of 15 minutes.

Answer is the way we arite our stream,

```
    stream_query = agg_df.writeStream.format("console") \
        .option("checkpointLocation", "checkpoint") \
        .outputMode("complete") \
        .option("truncate", "false") \
        .start()

    stream_query.awaitTermination()
```

We requested Spark to write with `Complete` mode. So Spark knows he cannot drop anything as you need every thing (Complete Mode). Thus `Watermark` is useless with `Complete` output mode.

Lets change the mode and see.

```
    stream_query = agg_df.writeStream.format("console") \
        .option("checkpointLocation", "checkpoint") \
        .outputMode("update") \
        .option("truncate", "false") \
        .start()

    stream_query.awaitTermination()
```

Now lets produce events.

```
(venv) apple@apples-MacBook-Air PysparkStreaming % ../../Softwares/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic spark-stream-05
>{ 'id': 'apple', 'amount': 10, 'type': 'buy', 'time': '20-04-2020-10-10' }
>{ 'id': 'dell', 'amount': 8, 'type': 'sell', 'time': '20-04-2020-10-40' }
>{ 'id': 'shell', 'amount': 18, 'type': 'buy', 'time': '20-04-2020-10-02' }
>{ 'id': 'dell', 'amount': 8, 'type': 'sell', 'time': '20-04-2020-09-30' }


```

Lets see what we get as an output.

```
-------------------------------------------
Batch: 1
-------------------------------------------
+------------------------------------------+------+
|window                                    |profit|
+------------------------------------------+------+
|[2020-04-20 10:10:00, 2020-04-20 10:15:00]|10    |
+------------------------------------------+------+

-------------------------------------------
Batch: 2
-------------------------------------------
+------+------+
|window|profit|
+------+------+
+------+------+

-------------------------------------------
Batch: 3
-------------------------------------------
+------------------------------------------+------+
|window                                    |profit|
+------------------------------------------+------+
|[2020-04-20 10:40:00, 2020-04-20 10:45:00]|-8    |
+------------------------------------------+------+

-------------------------------------------
Batch: 4
-------------------------------------------
+------+------+
|window|profit|
+------+------+
+------+------+

-------------------------------------------
Batch: 5
-------------------------------------------
+------+------+
|window|profit|
+------+------+
+------+------+

-------------------------------------------
Batch: 6
-------------------------------------------
+------+------+
|window|profit|
+------+------+
+------+------+
```

***Finally*** we get expected result.

> But note *Update* mode will work with updateable sink, i.e. database, nosql, record db, console. But sink like *File* will create duplicate records as it cannot updae inplace and new files will be created and thus will have duplicate records.


### Append mode with Aggregate Query (with Watermark)

We know Spark doesnot allow `Append` mode with aggregate query and throw `Analysis` exception. Reason is simple as Spark sopport Append mode when he knows rows send to sink will never change. Now for window based aggregation there are late arriving events which change previously published window agrregated result.

Scenario changes when you add watermark to windows aggregation, since after watermark interval the window aggregated result will never change (as subsequent late arriving event will never be considered) thus Spark will support Append mode.

But you must remember Spark will publish result for window only when it is elegible to be dropped (i.e. the window become old such that no further event can update the window). This will allow Spark to Append (only once) the row safely.

Updated Code.

```
    stream_query = agg_df.writeStream.format("console") \
        .option("checkpointLocation", "checkpoint") \
        .outputMode("append") \
        .option("truncate", "false") \
        .start()

    stream_query.awaitTermination()
```

Now lets produce events.

```
(venv) apple@apples-MacBook-Air PysparkStreaming % ../../Softwares/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic spark-stream-05
>{ 'id': 'apple', 'amount': 10, 'type': 'buy', 'time': '20-04-2020-10-10' }
>{ 'id': 'shell', 'amount': 18, 'type': 'buy', 'time': '20-04-2020-10-12' } 
>{ 'id': 'dell', 'amount': 8, 'type': 'sell', 'time': '20-04-2020-10-40' }
>{ 'id': 'shell', 'amount': 18, 'type': 'buy', 'time': '20-04-2020-10-02' }

```

Lets see what we get as an output.

```
-------------------------------------------
Batch: 0
-------------------------------------------
+------+------+
|window|profit|
+------+------+
+------+------+

-------------------------------------------
Batch: 1
-------------------------------------------
+------+------+
|window|profit|
+------+------+
+------+------+

-------------------------------------------
Batch: 2
-------------------------------------------
+------+------+
|window|profit|
+------+------+
+------+------+

-------------------------------------------
Batch: 3
-------------------------------------------
+------+------+
|window|profit|
+------+------+
+------+------+

-------------------------------------------
Batch: 4
-------------------------------------------
+------+------+
|window|profit|
+------+------+
+------+------+

-------------------------------------------
Batch: 5
-------------------------------------------
+------+------+
|window|profit|
+------+------+
+------+------+

-------------------------------------------
Batch: 6
-------------------------------------------
+------------------------------------------+------+
|window                                    |profit|
+------------------------------------------+------+
|[2020-04-20 10:10:00, 2020-04-20 10:15:00]|28    |
+------------------------------------------+------+

-------------------------------------------
Batch: 7
-------------------------------------------
+------+------+
|window|profit|
+------+------+
+------+------+
```

> Note its only on arrival of event with time `20-04-2020-10-40` which makes window `2020-04-20 10:10:00, 2020-04-20 10:15:00` out of scope and eligible to be dropped is when the window is passed to sink.
> All subsequent old event has no role in Streaming anyway.


