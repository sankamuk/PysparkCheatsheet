## Streaming

### 1. General Operations

- Running computation logic through an unbunded dataset.

- Challenges of Streaming system
	1. Maintaining large state of streaming data application
	2. Effectively and quickly deliver messages or events to application
	3. Out of order event handling
	4. Joining with batch data
	5. Exactly once guarantee
	6. Uneven data arrival rate

- Data Delivery Symentics: How a data arriving to stream processing engine is delivred to the application
	1. At most once, meaning no more than once (no dublicate) but can be data not getting delivered. DATA LOSS.
	2. At least once, meaning the data will surely be delivred but can be delivered more then once. DUPLICATE.
	3. Exactly once, most desirable but difficult to implement. Mostly achieved by checkpoint and write ahead log.

- Notion of Time: What and when about the data being processed.
	1. Event time, is the time when the data was generated and mostly embedded in the data itself.
	2. Processing time, is the time when the data was processed by the engine and based on the clock of the engine.
	Most desirable to consider event time only.

- Window: Based on the unbound nature of the data it is always streaming application computes data in a window, which most often is based on time(event time). Thus the stream of data is divided into chunk of data based on temporal boundary. Though the window of even time reflect real state of data set but it is often difficult to create the boundary as the unordered delivery of data because of unpredictability of medium of delivery.
	1. Tumbling window, where data set is divided into non overlapping set of time based data chunk. You just need a window time(interval) and every block has distinct start and end time, giving a new block every window interval.
	2. Sliding window, where data chunks are taken for block where time boundary for it moves continiously over the time line. Thus you need both and window time(interval) and sliding time(interval) and you get a new block every sliding interval. Single data will be part of multiple blocks.
	3. Session window, where chunks are not created on fixed time window but rather a based on inactivity which creats session boundary.

- Model: How engine trigger new computation cycle
	1. Record at a Time, every data initiates a computation and thus very low latency.
	2. Micro Batch, computation done with a batch getting accumulated, high latency but allow optimisation over data set.


### 2. Structured Streaming Overview

- Treating stream as table and new events as append of row to table.

- Transactional interaction to external storage system for imput/output data interaction.

- Support for Continious stream complementing the Micro Batch processing model for very low latency stream processor.

- Not all DataFrame operatins are applicable to Streaming context, i.e. distinct, limit, sort

- Components:
	1. Input source 
	2. Processing Logic (dataframe transformation)
	3. Output Mode
	4. Trigger
	5. Output Sink

- Sources
	1. Kafka
	2. File (Directory)
	3. Socket (Test)
	4. Rate (Test)

- Output Modes: What steam processed data is delivered to sink.
	1. Append, only newly created result is send to sink - Default
	2. Complete, the whole output send every time to sink
	3. Update, not only newly created but also updated result is send

- Trigger Type:
	1. Default, start next batch immediatly previous one finishes
	2. Fixed interval, process cycle triggered in fixed interval or immediatly after last one
	3. One Time, stops the stream after first batch is complete
	4. Continuous, low latency - Experimental 2.3

- Sink
	1. File, fault tolerant, support Append
	2. Kafka, fault tolerant, support Append, Update and Complete
	3. Foreach, custom implementation, all property depends on implementaton
	4. Console, not fault tolerant, support Append, Update and Complete
	5. Memory, not fault tolerant, support Append and Complete

- Watermark: it is the threshold for late data arrival based on event time after which arriving data with older event time can be discarded. This avoids stream system to indefinitely hold on old state for data arriving late.


### 3. Example 1

```
>>> from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
>>> mbscm = StructType([ StructField('id', StringType()), StructField('action', StringType()), StructField('ts', TimestampType())])
>>> 
>>> msdf = spark.readStream.schema(mbscm).json('/Users/apple/TEST/pyspark/stream/monitor/')
>>> from pyspark.sql.functions import window
>>> gbmsdf = msdf.groupBy(window(msdf['ts'], '10 minutes'), 'action').count()
>>> omsdf = gbmsdf.writeStream.format("console").option("truncate", "false").outputMode("complete").start()
-------------------------------------------                                     
Batch: 0
-------------------------------------------
+------------------------------------------+------+-----+
|window                                    |action|count|
+------------------------------------------+------+-----+
|[2018-03-02 10:00:00, 2018-03-02 10:10:00]|close |1    |
|[2018-03-02 10:00:00, 2018-03-02 10:10:00]|open  |3    |
+------------------------------------------+------+-----+

-------------------------------------------                                     
Batch: 1
-------------------------------------------
+------------------------------------------+------+-----+
|window                                    |action|count|
+------------------------------------------+------+-----+
|[2018-03-02 10:00:00, 2018-03-02 10:10:00]|close |2    |
|[2018-03-02 10:00:00, 2018-03-02 10:10:00]|open  |4    |
+------------------------------------------+------+-----+

-------------------------------------------                                     
Batch: 2
-------------------------------------------
+------------------------------------------+------+-----+
|window                                    |action|count|
+------------------------------------------+------+-----+
|[2018-03-02 10:10:00, 2018-03-02 10:20:00]|open  |1    |
|[2018-03-02 10:00:00, 2018-03-02 10:10:00]|close |3    |
|[2018-03-02 10:00:00, 2018-03-02 10:10:00]|open  |4    |
+------------------------------------------+------+-----+

>>> omsdf.stop()
```

- Different window unit


```
>>> msdf = spark.readStream.schema(mbscm).json('/Users/apple/TEST/pyspark/stream/monitor/')
>>> gbmsdf = msdf.groupBy(window(msdf['ts'], '3 minutes'), 'action').count()
>>> omsdf = gbmsdf.writeStream.format("console").option("truncate", "false").outputMode("complete").start()
>>> 
-------------------------------------------                                     
Batch: 0
-------------------------------------------
+------------------------------------------+------+-----+
|window                                    |action|count|
+------------------------------------------+------+-----+
|[2018-03-02 10:00:00, 2018-03-02 10:03:00]|open  |1    |
|[2018-03-02 10:03:00, 2018-03-02 10:06:00]|close |1    |
|[2018-03-02 10:03:00, 2018-03-02 10:06:00]|open  |2    |
+------------------------------------------+------+-----+

-------------------------------------------                                     
Batch: 1
-------------------------------------------
+------------------------------------------+------+-----+
|window                                    |action|count|
+------------------------------------------+------+-----+
|[2018-03-02 10:00:00, 2018-03-02 10:03:00]|open  |1    |
|[2018-03-02 10:03:00, 2018-03-02 10:06:00]|close |1    |
|[2018-03-02 10:03:00, 2018-03-02 10:06:00]|open  |2    |
|[2018-03-02 10:06:00, 2018-03-02 10:09:00]|open  |1    |
|[2018-03-02 10:06:00, 2018-03-02 10:09:00]|close |1    |
+------------------------------------------+------+-----+

-------------------------------------------                                     
Batch: 2
-------------------------------------------
+------------------------------------------+------+-----+
|window                                    |action|count|
+------------------------------------------+------+-----+
|[2018-03-02 10:00:00, 2018-03-02 10:03:00]|open  |1    |
|[2018-03-02 10:03:00, 2018-03-02 10:06:00]|close |2    |
|[2018-03-02 10:09:00, 2018-03-02 10:12:00]|open  |1    |
|[2018-03-02 10:03:00, 2018-03-02 10:06:00]|open  |2    |
|[2018-03-02 10:06:00, 2018-03-02 10:09:00]|open  |1    |
|[2018-03-02 10:06:00, 2018-03-02 10:09:00]|close |1    |
+------------------------------------------+------+-----+

>>> omsdf.stop()
```

- Query stream
```
>>> omsdf.status
{'message': 'Getting offsets from FileStreamSource[file:/Users/apple/TEST/pyspark/stream/monitor]', 'isDataAvailable': False, 'isTriggerActive': True}
>>> 

>>> omsdf.lastProgress
{'id': 'ab6bc301-309c-402b-88d8-1a08e7c56814', 'runId': '19f380f0-f47e-4944-bbc7-8e2beada6dcc', 'name': None, 'timestamp': '2020-03-01T12:13:49.028Z', 'batchId': 1, 'numInputRows': 0, 'inputRowsPerSecond': 0.0, 'processedRowsPerSecond': 0.0, 'durationMs': {'getOffset': 19, 'triggerExecution': 19}, 'stateOperators': [{'numRowsTotal': 2, 'numRowsUpdated': 0, 'memoryUsedBytes': 41991, 'customMetrics': {'loadedMapCacheHitCount': 0, 'loadedMapCacheMissCount': 0, 'stateOnCurrentVersionSizeBytes': 13191}}], 'sources': [{'description': 'FileStreamSource[file:/Users/apple/TEST/pyspark/stream/monitor]', 'startOffset': {'logOffset': 0}, 'endOffset': {'logOffset': 0}, 'numInputRows': 0, 'inputRowsPerSecond': 0.0, 'processedRowsPerSecond': 0.0}], 'sink': {'description': 'org.apache.spark.sql.execution.streaming.ConsoleSinkProvider@74fcb7f7'}}
             
>>> omsdf.isActive
True
```

- Blocking call for stream computation
```
>>> omsdf.awaitTermination()
```

### 4. Transformation

- Select and Filter

```
>>> msdf = spark.readStream.schema(mbscm).json('/Users/apple/TEST/pyspark/stream/monitor/')
>>> gbmsdf = msdf.filter(msdf['action'] == 'open').select('action', 'id')
>>> omsdf = gbmsdf.writeStream.format("console").option("truncate", "false").outputMode("append").start()
>>> -------------------------------------------
Batch: 0
-------------------------------------------
+------+------+
|action|id    |
+------+------+
|open  |phone1|
|open  |phone2|
|open  |phone3|
+------+------+

-------------------------------------------
Batch: 1
-------------------------------------------
+------+------+
|action|id    |
+------+------+
|open  |phone4|
+------+------+


>>> msdf = spark.readStream.schema(mbscm).json('/Users/apple/TEST/pyspark/stream/monitor/')
>>> msdf.createOrReplaceTempView('stream_df')
>>> spark.sql('select * from stream_df where action == "open"')
DataFrame[id: string, action: string, ts: timestamp]
>>> omsdf = msdf.writeStream.format("console").option("truncate", "false").outputMode("append").start()
>>> -------------------------------------------
Batch: 0
-------------------------------------------
+------+------+-------------------+
|id    |action|ts                 |
+------+------+-------------------+
|phone1|open  |2018-03-02 10:02:33|
|phone2|open  |2018-03-02 10:03:35|
|phone3|open  |2018-03-02 10:03:50|
|phone1|close |2018-03-02 10:04:35|
+------+------+-------------------+
```

### 5. Join

- Stream to Stream join (Supported inner, and conditional left and right outer)
```
>>> mhscm = StructType([ StructField('id', StringType()), StructField('health', StringType()), StructField('ts', TimestampType())])

>>> msdf = spark.readStream.schema(mbscm).json('/Users/apple/TEST/pyspark/stream/monitor/')
>>> mhdf = spark.readStream.schema(mhscm).json('/Users/apple/TEST/pyspark/stream/health/')

>>> msdfwm = msdf.withWatermark("ts", "1 hours")
>>> mhdfwm = mhdf.withWatermark("ts", "1 hours")

>>> joindf = msdfwm.join(mhdfwm, msdfwm['id'] == mhdfwm['id'], 'inner')


>>> omsdf = joindf.writeStream.format("console").option("truncate", "false").outputMode("append").start()

apples-MacBook-Air:pyspark apple$ cat stream/input/f1.json; cp stream/input/f1.json stream/monitor/
{"id":"phone1","action":"open","ts":"2018-03-02T10:02:33"}
{"id":"phone2","action":"open","ts":"2018-03-02T10:03:35"}
{"id":"phone3","action":"open","ts":"2018-03-02T10:03:50"}
{"id":"phone1","action":"close","ts":"2018-03-02T10:04:35"}
```
OUTPUT
```
-------------------------------------------                                     
Batch: 0
-------------------------------------------
+---+------+---+---+------+---+
|id |action|ts |id |health|ts |
+---+------+---+---+------+---+
+---+------+---+---+------+---+
```
=> Pushing data in second Dataframe
```
apples-MacBook-Air:pyspark apple$ cat stream/input/e1.json ; cp stream/input/e1.json stream/health/
{"id":"phone1","health":"good","ts":"2018-03-02T10:02:33"}
{"id":"phone2","health":"good","ts":"2018-03-02T10:03:35"}
{"id":"phone3","health":"good","ts":"2018-03-02T10:03:50"}
{"id":"phone1","health":"bad","ts":"2018-03-02T11:14:35"}
```
OUTPUT
```
-------------------------------------------                                     
Batch: 1
-------------------------------------------
+------+------+-------------------+------+------+-------------------+
|id    |action|ts                 |id    |health|ts                 |
+------+------+-------------------+------+------+-------------------+
|phone3|open  |2018-03-02 10:03:50|phone3|good  |2018-03-02 10:03:50|
|phone1|open  |2018-03-02 10:02:33|phone1|good  |2018-03-02 10:02:33|
|phone1|close |2018-03-02 10:04:35|phone1|good  |2018-03-02 10:02:33|
|phone1|open  |2018-03-02 10:02:33|phone1|bad   |2018-03-02 11:14:35|
|phone1|close |2018-03-02 10:04:35|phone1|bad   |2018-03-02 11:14:35|
|phone2|open  |2018-03-02 10:03:35|phone2|good  |2018-03-02 10:03:35|
+------+------+-------------------+------+------+-------------------+
```
=> Adding new data
apples-MacBook-Air:pyspark apple$ cat stream/input/f2.json; cp stream/input/f2.json stream/monitor/
{"id":"phone3","action":"close","ts":"2018-03-02T10:07:35"}
{"id":"phone4","action":"open","ts":"2018-03-02T10:07:50"}
```
OUTPUT
```
-------------------------------------------                                     
Batch: 2
-------------------------------------------
+------+------+-------------------+------+------+-------------------+
|id    |action|ts                 |id    |health|ts                 |
+------+------+-------------------+------+------+-------------------+
|phone3|close |2018-03-02 10:07:35|phone3|good  |2018-03-02 10:03:50|
+------+------+-------------------+------+------+-------------------+
```

=> Adding out of Watermark data not considered
apples-MacBook-Air:pyspark apple$ cat stream/input/f4.json; cp stream/input/f4.json stream/monitor/
{"id":"phone3","action":"close","ts":"2018-03-02T04:07:35"}
{"id":"phone4","action":"open","ts":"2018-03-02T05:07:50"}
```
OUTPUT
```
-------------------------------------------                                     
Batch: 3
-------------------------------------------
+---+------+---+---+------+---+
|id |action|ts |id |health|ts |
+---+------+---+---+------+---+
+---+------+---+---+------+---+
```

=> Adding data in second DF
```
apples-MacBook-Air:pyspark apple$ cat stream/input/e2.json; cp stream/input/e2.json stream/health/
{"id":"phone4","health":"good","ts":"2018-03-02T10:12:33"}
```
OUTPUT
```
-------------------------------------------                                     
Batch: 4
-------------------------------------------
+------+------+-------------------+------+------+-------------------+
|id    |action|ts                 |id    |health|ts                 |
+------+------+-------------------+------+------+-------------------+
|phone4|open  |2018-03-02 10:07:50|phone4|good  |2018-03-02 10:12:33|
+------+------+-------------------+------+------+-------------------+
```

- Stream and Static Join

```
>>> staticdf = spark.read.schema(mhscm).json('/Users/apple/TEST/pyspark/stream/health/')
>>> staticdf.show()
+------+------+-------------------+
|    id|health|                 ts|
+------+------+-------------------+
|phone1|  good|2018-03-02 10:02:33|
|phone2|  good|2018-03-02 10:03:35|
|phone3|  good|2018-03-02 10:03:50|
|phone1|   bad|2018-03-02 11:14:35|
|phone4|  good|2018-03-02 10:12:33|
+------+------+-------------------+

>>> msdf = spark.readStream.schema(mbscm).json('/Users/apple/TEST/pyspark/stream/monitor/')
>>> msdfwm = msdf.withWatermark("ts", "1 hours")
>>> 
>>> joindf = staticdf.join(msdfwm, staticdf['id'] == msdfwm['id'], 'inner')
>>> 
>>> omsdf = joindf.writeStream.format("console").option("truncate", "false").outputMode("append").start()
```
=> Adding data
```
apples-MacBook-Air:pyspark apple$ cp stream/input/f1.json stream/monitor/
```
OUTPUT
```
>>> -------------------------------------------
Batch: 0
-------------------------------------------
+------+------+-------------------+------+------+-------------------+
|id    |health|ts                 |id    |action|ts                 |
+------+------+-------------------+------+------+-------------------+
|phone1|good  |2018-03-02 10:02:33|phone1|close |2018-03-02 10:04:35|
|phone1|good  |2018-03-02 10:02:33|phone1|open  |2018-03-02 10:02:33|
|phone2|good  |2018-03-02 10:03:35|phone2|open  |2018-03-02 10:03:35|
|phone3|good  |2018-03-02 10:03:50|phone3|open  |2018-03-02 10:03:50|
|phone1|bad   |2018-03-02 11:14:35|phone1|close |2018-03-02 10:04:35|
|phone1|bad   |2018-03-02 11:14:35|phone1|open  |2018-03-02 10:02:33|
+------+------+-------------------+------+------+-------------------+
```

=> Adding data
```
apples-MacBook-Air:pyspark apple$ cp stream/input/f4.json stream/monitor/
```
OUTPUT
```
-------------------------------------------
Batch: 1
-------------------------------------------
+------+------+-------------------+------+------+-------------------+
|id    |health|ts                 |id    |action|ts                 |
+------+------+-------------------+------+------+-------------------+
|phone3|good  |2018-03-02 10:03:50|phone3|close |2018-03-02 04:07:35|
|phone4|good  |2018-03-02 10:12:33|phone4|open  |2018-03-02 05:07:50|
+------+------+-------------------+------+------+-------------------+
```

### 6. Output Modes

- Stateless stream query: performs transformation before writting data to sink. Cleaning, Filter, Transforming, Partitioning, Saving in new format. Only APPEND output mode is applicable.
	* Error for wrong output mode : org.apache.spark.sql.AnalysisException: Complete output mode not supported when there are no streaming aggregations on streaming DataFrames/Datasets;

- Stateful stream query: includes aggregation and need to preserve state. Here the aggregated state need to be maintained to process output with the newly arrived data along with previous aggregation output to get new output. COMPLETE and UPDATE output modes are appropriate for the stateful query type with the aggregation state implicitly maintained by the Structured Streaming engine.
	* Error for wrong output mode : org.apache.spark.sql.AnalysisException: Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark;

- Statefull with Watermark: with aggregation with watermark APPEND output mode is applicable. This is because after watermark old state is removed and new states are added.


### 7. Trigger 

NOTE: For any trigger mode to act you need data, thus computation in streaming will never start without data even if time based triggering is set.

- Default: Microbath mode where new trigger is based on previous batch finish.

- Specific Time: 

```
>>> msdf = spark.readStream.schema(mbscm).json('/Users/apple/TEST/pyspark/stream/monitor/')
>>> opdf = msdf.writeStream.outputMode("append").format("console").trigger(processingTime='50 seconds').start()
```
=> Copy data
```
apples-MacBook-Air:pyspark apple$ cp stream/input/f3.json stream/input/f4.json stream/monitor/
```
=> Processing start after 50 seconds
```
>>> -------------------------------------------
Batch: 0
-------------------------------------------
+------+------+-------------------+
|    id|action|                 ts|
+------+------+-------------------+
|phone3| close|2018-03-02 04:07:35|
|phone4|  open|2018-03-02 05:07:50|
|phone2| close|2018-03-02 10:04:50|
|phone5|  open|2018-03-02 10:10:50|
+------+------+-------------------+
```

- One time trigger: automatically stops after processing the dataset like batch

=> Copy data
```
apples-MacBook-Air:pyspark apple$ cp stream/input/f3.json stream/input/f4.json stream/monitor/
```
Output

```
>>> msdf = spark.readStream.schema(mbscm).json('/Users/apple/TEST/pyspark/stream/monitor/')
>>> opdf = msdf.writeStream.outputMode("append").format("console").trigger(once=True).start()
>>> -------------------------------------------
Batch: 0
-------------------------------------------
+------+------+-------------------+
|    id|action|                 ts|
+------+------+-------------------+
|phone3| close|2018-03-02 04:07:35|
|phone4|  open|2018-03-02 05:07:50|
|phone2| close|2018-03-02 10:04:50|
|phone5|  open|2018-03-02 10:10:50|
+------+------+-------------------+

opdfopdf.status
{'message': 'Stopped', 'isDataAvailable': False, 'isTriggerActive': False}
```

- Continuous: No support for aggregation as data once arrived get processed and written to sink, only supports Kafka and Rate source

=> Below we use continuous mode with 2 seconds checkpoint interval with rate source

```
>>> msdf = spark.readStream.format('rate').option("numPartitions","2").load()
>>> opdf = msdf.writeStream.outputMode("append").format("console").trigger(continuous='1 second').start()
>>> -------------------------------------------
Batch: 0
-------------------------------------------
+---------+-----+
|timestamp|value|
+---------+-----+
+---------+-----+

-------------------------------------------
Batch: 1
-------------------------------------------
+--------------------+-----+
|           timestamp|value|
+--------------------+-----+
|2020-03-08 00:24:...|    0|
|2020-03-08 00:24:...|    1|
+--------------------+-----+
```

### 8. Event Time Based Processing

- Fixed Window Aggregation

```
>>> from pyspark.sql.functions import window
>>> msdf = spark.readStream.schema(mbscm).json('/Users/apple/TEST/pyspark/stream/monitor/')
>>> gbmsdf = msdf.groupBy(window(msdf['ts'], '5 minutes'), 'action').count()
>>> omsdf = gbmsdf.writeStream.format("console").option("truncate", "false").outputMode("complete").start()
>>> 
-------------------------------------------                                     
Batch: 0
-------------------------------------------
+------------------------------------------+------+-----+
|window                                    |action|count|
+------------------------------------------+------+-----+
|[2018-03-02 10:00:00, 2018-03-02 10:05:00]|open  |3    |
|[2018-03-02 10:00:00, 2018-03-02 10:05:00]|close |1    |
+------------------------------------------+------+-----+
```

- Sliding window

```
>>> msdf = spark.readStream.schema(mbscm).json('/Users/apple/TEST/pyspark/stream/monitor/')
>>> gbmsdf = msdf.groupBy(window(msdf['ts'], '10 minutes', '5 minutes'), 'action').count()
>>> omsdf = gbmsdf.writeStream.format("console").option("truncate", "false").outputMode("complete").start()
-------------------------------------------                                     
Batch: 0
-------------------------------------------
+------------------------------------------+------+-----+
|window                                    |action|count|
+------------------------------------------+------+-----+
|[2018-03-02 10:00:00, 2018-03-02 10:10:00]|close |1    |
|[2018-03-02 10:00:00, 2018-03-02 10:10:00]|open  |3    |
|[2018-03-02 09:55:00, 2018-03-02 10:05:00]|close |1    |
|[2018-03-02 09:55:00, 2018-03-02 10:05:00]|open  |3    |
+------------------------------------------+------+-----+
```

- State Maintainance

The intermediate state is stored in an in-memory, versioned, key-value “state store” on the Spark executors, and it is written out to a write-ahead log, which should be configured to reside in a stable storage system like HDFS. At every trigger point, the state is read and updated in the in-memory state store and then written out to the write-ahead log. In the case of a failure and when a Spark Structured Streaming application is restarted, the state is restored from the write-ahead log and resumes from that point. This fault-tolerant state management obviously incurs some resource and processing overhead in the Structured Streaming engine. The amount of overhead is proportional to the amount of state it needs to maintain Therefore, it is important keep the amount of state in an acceptable size; in other words, the size of the state should not grow indefinitely.

Watermarking is a commonly used technique in streaming processing engines to deal with late data as well as to limit the amount of state needed to maintain it.

Condition for Watermarking
	1. The output mode can’t be the complete mode and must be in either update or append mode.
	2. The aggregation via the groupBy transformation must be directly on the event-time column or a window on the event- time column.
	3. The event-time column specified in the Watermark API and the groupBy transformation must be the same one.
	4. When setting up a streaming DataFrame, the Watermark API must be called before the groupBy transformation is called; otherwise, it will be ignored.


- Arbitrary Stateful Processing - NOT SUPPORTED
https://databricks.com/blog/2017/10/17/arbitrary-stateful-processing-in-apache-sparks-structured-streaming.html



### 9. Duplicates

- Performing data duplication without specifying a watermark is that the state that Structured Streaming needs to maintain will grow infinitely over the lifetime of your streaming application, and this may lead to out-of-memory issues. With watermarking, late data older than the watermark will be automatically dropped to avoid any possibility of duplicates.

```
>>> msdf = spark.readStream.schema(mbscm).json('/Users/apple/TEST/pyspark/stream/monitor/')
>>> msdfwm = msdf.withWatermark('ts', '6 hours').dropDuplicates(['id', 'ts'])
>>> gbmsdf = msdfwm.groupBy('action').count()
>>> omsdf = gbmsdf.writeStream.format("console").option("truncate", "false").outputMode("complete").start()
```

=> Copy input
```
apples-MacBook-Air:pyspark apple$ cat stream/input/f4.json ; cp stream/input/f4.json stream/monitor/
{"id":"phone3","action":"close","ts":"2018-03-02T04:07:35"}
{"id":"phone3","action":"close","ts":"2018-03-02T04:07:35"}
{"id":"phone4","action":"open","ts":"2018-03-02T05:07:50"}
```
Output

```
-------------------------------------------                                     
Batch: 0
-------------------------------------------
+------+-----+
|action|count|
+------+-----+
|close |1    |
|open  |1    |
+------+-----+
```


### 10. Fault Tolerance with checkpointing

```
>>> omsdf = gbmsdf.writeStream.format("console").option("truncate", "false").option("checkpointLocation",
                                           "/reliable/location").outputMode("complete").start()
```

- Checkpointing can preserve state over a restart even with 
	1. Streaming application change (incase not compatible will throw exception)
	2. Spark runtime change (in case not compatible check release note before upgrade spark binary)


### 11. Monitoring

```
>>> omsdf.status
{'message': 'Getting offsets from FileStreamSource[file:/Users/apple/TEST/pyspark/stream/monitor]', 'isDataAvailable': False, 'isTriggerActive': True}
>>> 

>>> omsdf.lastProgress
{'id': 'ab6bc301-309c-402b-88d8-1a08e7c56814', 'runId': '19f380f0-f47e-4944-bbc7-8e2beada6dcc', 'name': None, 'timestamp': '2020-03-01T12:13:49.028Z', 'batchId': 1, 'numInputRows': 0, 'inputRowsPerSecond': 0.0, 'processedRowsPerSecond': 0.0, 'durationMs': {'getOffset': 19, 'triggerExecution': 19}, 'stateOperators': [{'numRowsTotal': 2, 'numRowsUpdated': 0, 'memoryUsedBytes': 41991, 'customMetrics': {'loadedMapCacheHitCount': 0, 'loadedMapCacheMissCount': 0, 'stateOnCurrentVersionSizeBytes': 13191}}], 'sources': [{'description': 'FileStreamSource[file:/Users/apple/TEST/pyspark/stream/monitor]', 'startOffset': {'logOffset': 0}, 'endOffset': {'logOffset': 0}, 'numInputRows': 0, 'inputRowsPerSecond': 0.0, 'processedRowsPerSecond': 0.0}], 'sink': {'description': 'org.apache.spark.sql.execution.streaming.ConsoleSinkProvider@74fcb7f7'}}
             
>>> omsdf.isActive
True
```

- Note for a stable streaming system inputRowsPerSecond should always be less than equal to processedRowsPerSecond.
- If you are doing statefull aggregation and system maintains state keep eye on stateOperators and its resource requirements.

