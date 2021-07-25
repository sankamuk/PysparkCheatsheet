# Pyspark Structured Streaming - Part 01

Below is the issues with classic Spark batch jobs to handle streaming by running frequent batches over the data.

- Should we run in incremental fashion and save and manage the output of all previous batches. Or we treat every batch idempotent and process all data from the very begining until the current batch.
- Should we validate whether previous batch state (has it finished) or we do not care.
- Should we validate the data is complete for the current batch or we do not care.
- How do we track the data already processed and need to be processed in current batch.
- Should we bother about the failure that happened in previous batch or not.
- How do we handle late arriving data, data whose actual event time is earlier but reported to system in later time because of various environment or system issue.

Handling all these is not new issue but it just gets complecated when the batch becomes smaller and frequent. 

The elegant solution to all such problems in Spark is Streaming. The idea that Spark creator advocated is Sream processing is just about handing some additional problems cases over the already solved batch processing problems. Thus Spark Streaming API has been build upon the batch processing API with additional capabilities.

Thus Spark Streaming offer the below out of the box.

- Scheduling of batches.
- Data management for batches.
- Intermediate state management across batches.
- Combine states across batches.
- Batch failure and job failure management.

Spark Structured Streaming API add the below features over the old DStream API.

- DStream API is deprecated and no new development to be done with it.
- Unified Dataframe based data processing framework for both batch and streaming data.
- Avalability of Catalyst optimiser which works with SQL engine processing Dataframe.
- Support for EVENT time based processing rather than incorrect process time based processing.


## First Job (From Socket)

Session Creation

```
    spark = SparkSession \
            .builder \
            .appName("Word Count") \
            .config("spark.sql.shuffle.partitions", 2) \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .getOrCreate()
```
 
Configure streaming source
 
```
     df = spark \
        .readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", "9999") \
        .load()
```
 
> Note we use socket source where Spark will read data from socket.
 
Configure stream sink

```
    stream = df.writeStream.format("console") \
        .option("checkpointLocation", "checkpoint") \
        .outputMode("append") \
        .start()
```

> Since output mode is `append` only new data will be written to the sink.

Start the socket which Spark will listen.

```
(venv) apple@apples-MacBook-Air PysparkStreaming % nc -lk 9999 <ENTER>
hello <ENTER>
hello world <ENTER>

```

In console where job running you should get output as below.

```
-------------------------------------------
Batch: 1
-------------------------------------------
+-----+
|value|
+-----+
|hello|
+-----+

-------------------------------------------
Batch: 2
-------------------------------------------
+-----------+
|      value|
+-----------+
|hello world|
+-----------+
```

> Note the flow had no transformation, but you can run any Dataframe transformations (there are restrictions in most `action`).


### Added transformation

We do word count.

```
    df_w = df.selectExpr("explode(split(value, ' ')) as words") \
        .groupby('words') \
        .count()
```

> Note now since we have aggregation we can do a output of complete mode. More on this later.

```
    stream = df_w.writeStream.format("console") \
        .option("checkpointLocation", "checkpoint") \
        .outputMode("complete") \
        .start()
```

## Triggering

In above case if you noticed the job was getting for every line of input submitted. Thus every line submitted triggered a `micro-batch`. This may not be what we want and may be we want Spark to collect some data before triggering next batch. This is what Trigger helps us achieve.

**Options**:

- `Unspecified`: A new batch triggered with every input. This is **default** option. Note if current batch takes time to process next batch will be triggered once current batch finish.
- `Time`: A job is triggered after every time interval. All data collected until then will be processed once. Note like above for this option also if previous batch takes time next batch will have to wait to be triggered.
- `Once`: This is like a single batch job. The job triggers once process all data until then and finish.
- `Continuous`: Currently experimental but it is Spark's attempt to real stream processing. We do not discuss this much as not yet production ready.

Below we update our job and convert it into time based trigger.

```
    stream = df_w.writeStream.format("console") \
        .option("checkpointLocation", "checkpoint") \
        .outputMode("complete") \
        .trigger("1 minute") \
        .start()
```

**Output**

```
(venv) apple@apples-MacBook-Air PysparkStreaming % nc -lk 9999
hello world
again hello
world order
```

**Result**

```
-------------------------------------------
Batch: 0
-------------------------------------------
+-----+-----+
|words|count|
+-----+-----+
+-----+-----+

-------------------------------------------
Batch: 1
-------------------------------------------
+-----+-----+
|words|count|
+-----+-----+
|world|    2|
|hello|    2|
|order|    1|
|again|    1|
+-----+-----+
```
> Note how all lines comes in one batch together.


## File Source

Defining the source is not much different.

```
    input_df = spark \
        .readStream \
        .format("json") \
        .option("path", "input/file/") \
        .load()
```

Followup with your transformation.

```
    transformed_df = input_df \
        .withColumn("_country", when( col("country").isNull(), "Unknown").otherwise( col("country") ) ) \
        .drop("country") \
        .withColumnRenamed("_country", "country")
```

> Here we do simple transformation in replacing blank country name with `Unknown`.

Lets complete our job by writting the output to a `file sink`.

```
    stream_query = transformed_df.writeStream.format("json") \
        .option("checkpointLocation", "checkpoint") \
        .option("path", "output/file") \
        .outputMode("append") \
        .trigger( processingTime="50 seconds" ) \
        .start()

    stream_query.awaitTermination()
```

### Providing schema

You have two option to provide schema.

**Option 1**: Session level configuration `spark.sql.streaming.schemaInference` to infer schema.

```
    spark = SparkSession \
            .builder \
            .appName("File Source 01") \
            .config("spark.sql.shuffle.partitions", 2) \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .getOrCreate()
```

> Note this setting needs atleast some data file to be present before job starts. Else you get below error.

```
pyspark.sql.utils.AnalysisException: Unable to infer schema for JSON. It must be specified manually.
```

**Option 2**: Read data with schema validation.

```
    schema1 = StructType(
        [StructField('id', LongType()), StructField('name', StringType()), StructField('city', StringType()),
         StructField('country', StringType())])

    input_df = spark \
        .readStream \
        .format("json") \
        .option("path", "input/file/") \
        .schema(schema1) \
        .load()
```

Providing input.

```
(venv) apple@apples-MacBook-Air PysparkStreaming % cp data/data1.json input/file   
(venv) apple@apples-MacBook-Air PysparkStreaming % cat input/file/data1.json 
{ "id": 1, "name": "Sankar", "city": "kolkata", "country": "india" }
{ "id": 1, "name": "Kun", "city": "tiangen" }
(venv) apple@apples-MacBook-Air PysparkStreaming %
```

Generated result.

```
(venv) apple@apples-MacBook-Air PysparkStreaming % ls -ltr output/file 
total 8
-rw-r--r--  1 apple  staff  119 Apr 17 18:31 part-00000-64ff6d84-0f81-4aa0-ad08-85c94e42c71e-c000.json
drwxr-xr-x  3 apple  staff   96 Apr 17 18:31 _spark_metadata
(venv) apple@apples-MacBook-Air PysparkStreaming % cat output/file/part-00000-64ff6d84-0f81-4aa0-ad08-85c94e42c71e-c000.json 
{"id":1,"name":"Sankar","city":"kolkata","country":"india"}
{"id":1,"name":"Kun","city":"tiangen","country":"Unknown"}
```

### Input housekeeping

In case you want to cleanup your input location you can use the following option.

```
    input_df = spark \
        .readStream \
        .format("json") \
        .option("path", "input/file/") \
        .option("cleanSource", "delete") \
        .schema(schema1) \
        .load()
```

Below are important things to keep in mind.

- Files processed in previous batch will be cleaned up at the start of next batch.
- You can also archive file rather than delete with archieve option.
- Deletion or moving files will incur processing time and might increase up your batch time. You can even handle this cleaning yourself with custom cleanup job.
- Inside checkpoint directory there will be a `source` folder with files named after batch number, this contains file names processed in that batch.

