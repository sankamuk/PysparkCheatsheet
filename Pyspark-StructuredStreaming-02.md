# Pyspark Structured Streaming - Part 2

## Output Modes

In a streaming system there are a list of input records for each batch with get processed and forms an result set which is an cumulative result considering not only the input of the current batch input but also previous batched cumulative output forming a new output. Here is where Output Modes comes in, though the streaming system manages the cumulative output result in its state store but you might control result that get passed into the sink.

- You might want to pass the complete cumulative result to the sink, defined as `Complete` output mode.
- You might want to pass only the updated records from the complete cumulative result to the sink, defined as `Update` output mode.
- There is a special third case when you know there can never be update possible to the existing cumulative result only new records gets appended, in which case you might choose to only pass on the new records to the sink. This is called `Insert` output mode.

> Note inappropiate output mode can be detected by Spark throwing an `AnalysisException`.

**Note**

- Append is applicable only for non aggregated stream and the result need to be immutable.
- Append for agreegated streaming query is supported in case when whatermark is applied. Record is only pushed to sink on finalization of intermediate state end of watermark interval.
- Complete mode in non agreegated streaming query also is invalid (AnalysisException) as there is no concert of cumulative output in such case.
- Note update mode in non agreegated streaming query also is valid and acts as append mode.


***Examples***

Input Batch 1:

```
> hello world
> my world
```

Output Batch 1:

*Complete Mode*

```
hello 1
world 2
my  1
```

*Update Mode*

```
hello 1
world 2
my  1
```

Input Batch 2:

```
> hello again
```

Output Batch 1:

*Complete Mode*

```
hello 2
world 2
my  1
again 1
```

*Update Mode*

```
hello 2
again 1
```


## Kafka Source

It is not really difficult and Kafka is just another source for Spark. Lets see the source defination.

```
    input_df = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", "spark-stream-01") \
                .option("startingOffsets", "earliest") \
                .load()
```

Now the resultant dataframe will have a value column with the message from topic in binary format we will have to cast it appropiately. In our example it should be a string encoded JSON. Thus after casting we need to convert the string to JSON and then normalize/flatten it.

```
    result_df = input_df.select( col("value").cast("string") ) \
                    .withColumn("value_json", from_json(col("value"), schema1)) \
                    .select("value_json.*")
```

Lets write this to console in append mode.

```
    stream_query = result_df.writeStream.format("console") \
                .option("checkpointLocation", "checkpoint") \
                .outputMode("append") \
                .start()

    stream_query.awaitTermination()
```

### Spark Structured Streaming and Kafka integration

Before we can run we need to handle one important thing. Kafka support for Spark is not out-of-the-box thus we need to make our Spark run time avail the Kafka dependency.

**Option 1**: Adding dependency during startup.

```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0
```

**Option 2**: Adding dependency in configuration *spark-defaults.conf*.

```
spark.jars.packages   org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0
```

> In both above option the dependency will be downloaded during job startup.

**Option 3**: Statically download dependent jars and set classpath to make jars available to Spark runtime.

Step1: To get all dependent jars just run the below job, at output you will find the location of the cached location of downloaded jars.

```
from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("Kafka Source Test") \
        .config("spark.sql.shuffle.partitions", 2) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
        .getOrCreate()
```

The output will contain the dependent jars and its local cached location.

```
...
Ivy Default Cache set to: /Users/apple/.ivy2/cache
The jars for the packages stored in: /Users/apple/.ivy2/jars
...
:: resolution report :: resolve 1036ms :: artifacts dl 46ms
	:: modules in use:
	com.github.luben#zstd-jni;1.4.4-3 from central in [default]
	org.apache.commons#commons-pool2;2.6.2 from central in [default]
	org.apache.kafka#kafka-clients;2.4.1 from central in [default]
	org.apache.spark#spark-sql-kafka-0-10_2.12;3.0.0 from central in [default]
	org.apache.spark#spark-token-provider-kafka-0-10_2.12;3.0.0 from central in [default]
	org.lz4#lz4-java;1.7.1 from central in [default]
	org.slf4j#slf4j-api;1.7.30 from central in [default]
	org.spark-project.spark#unused;1.0.0 from central in [default]
	org.xerial.snappy#snappy-java;1.1.7.5 from central in [default]
...
```

Now get all jars and put it in <JAR_HOME> location in both driver and executor machines.

```
(venv) apple@apples-MacBook-Air PysparkStreaming % spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 jars.py 2> job.output
(venv) apple@apples-MacBook-Air PysparkStreaming % cat job.output | awk '{ if ( $1 == "found" ) print $2 }' | awk -F# '{ print $1 }' | while read fld
do
find /Users/apple/.ivy2/cache/${fld} -type f -name '*.jar' -exec cp {} /Users/apple/PycharmProjects/jars/ \;
done
(venv) apple@apples-MacBook-Air PysparkStreaming % ls -ltr /Users/apple/PycharmProjects/jars/
total 20824
-rw-r--r--  1 apple  staff   348853 Apr 18 13:23 spark-sql-kafka-0-10_2.12-3.0.0.jar
-rw-r--r--  1 apple  staff    56312 Apr 18 13:23 spark-token-provider-kafka-0-10_2.12-3.0.0.jar
-rw-r--r--  1 apple  staff  3269712 Apr 18 13:23 kafka-clients-2.4.1.jar
-rw-r--r--  1 apple  staff  4210625 Apr 18 13:23 zstd-jni-1.4.4-3.jar
-rw-r--r--  1 apple  staff   649950 Apr 18 13:23 lz4-java-1.7.1.jar
-rw-r--r--  1 apple  staff  1934320 Apr 18 13:23 snappy-java-1.1.7.5.jar
-rw-r--r--  1 apple  staff    41472 Apr 18 13:23 slf4j-api-1.7.30.jar
-rw-r--r--  1 apple  staff     2777 Apr 18 13:23 unused-1.0.0.jar
-rw-r--r--  1 apple  staff   129174 Apr 18 13:23 commons-pool2-2.6.2.jar
```

> Note above `/Users/apple/PycharmProjects/jars/` is your <JAR_HOME>.

Step2: Mention <JAR_HOME> in your job classpath.

```
    spark = SparkSession \
            .builder \
            .appName("Kafka Source 01") \
            .config("spark.sql.shuffle.partitions", 2) \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.executor.extraClassPath", "/Users/apple/PycharmProjects/jars/*") \
            .config("spark.driver.extraClassPath", "/Users/apple/PycharmProjects/jars/*") \
            .getOrCreate()
```

### Execution

Start Zookeeper and Kafka. Create topic and publish message.

```
(venv) apple@apples-MacBook-Air PysparkStreaming % ../../Softwares/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic spark-stream-01 --partitions 1 --replication-factor 1
Created topic spark-stream-01.
(venv) apple@apples-MacBook-Air PysparkStreaming % ../../Softwares/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic spark-stream-01
>{ "id": 1, "name": "Sankar", "city": "kolkata", "country": "india" }
>{ "id": 1, "name": "Kun", "city": "tiangen" }
>{ "id": 4, "name": "Xun", "country": "china" }
>{ "id": 3, "name": "Jade", "city": "berut", "country": "lebanon" }
>
```

Our job should output as below.

```
-------------------------------------------
Batch: 0
-------------------------------------------
+---+------+-------+-------+
| id|  name|   city|country|
+---+------+-------+-------+
|  1|Sankar|kolkata|  india|
|  1|   Kun|tiangen|   null|
|  4|   Xun|   null|  china|
|  3|  Jade|  berut|lebanon|
+---+------+-------+-------+
```

> Publish new message you will be getting new batch.

***Incase you want to test transformation of your jobs step by step with show (which is not supported with Streaming Source and will thrwo an AnalysisException). Trick is to replace your source from readStream to plain read as below.***

```
    input_df = spark.read \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", "spark-stream-01") \
                .option("startingOffsets", "earliest") \
                .load()

    result_df = input_df.select( col("value").cast("string") ) \
                    .withColumn("value_json", from_json(col("value"), schema1)) \
                    .select("value_json.*")

    result_df.show()
```
***Once you have tested your complete transformation chain replace back read with readStream and remove show() and start your Streaming query as usual with sink defination.***


## Kafka Sink

Only important thing to note why writting to Kafka is that you need to serialize your row data as a column named value and need to provide a column for key.

```
    input_df = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", "spark-stream-01") \
                .option("startingOffsets", "earliest") \
                .load()

    transformed_df = input_df.select( col("value").cast("string") ) \
                    .withColumn("value_json", from_json(col("value"), schema1)) \
                    .select("value_json.*")

    result_df = transformed_df.selectExpr("name as key",
                                          """
                                          to_json(named_struct(
                                          'id', id,
                                          'name', name,
                                          'city', city,
                                          'country', country
                                          )) as value
                                          """)
```

> Note we have two methord (struct and named_struct) to create a structure from a list of column, this can be fed to to_json or to_csv for serialization. The difference between struct and named_struct is that named_struct allow you to rename the columns. 

Now you can create the streaming query and add Kafka sink.

```
    stream_query = result_df.writeStream \
                    .format("kafka") \
                    .queryName("Kafka Writter 02") \
                    .option("kafka.bootstrap.servers", "localhost:9092") \
                    .option("topic", "spark-stream-02") \
                    .option("checkpointLocation", "checkpoint") \
                    .outputMode("append") \
                    .start()

    stream_query.awaitTermination()
```

> Note adding `queryName` will place this name in Description column of your Spark UI Jobs tab and Name column in Structured Streaming tab of UI.


## Multiple Query Application

You can run two query in same application with two below important settings.

***Two streaming query should not share a checkpoint directory***

```
# Query 01
stream_query_01 = result_df.writeStream \
                    .format("kafka") \
                    .queryName("Kafka Writter 02") \
                    .option("kafka.bootstrap.servers", "localhost:9092") \
                    .option("topic", "spark-stream-02") \
                    .option("checkpointLocation", "checkpoint_01") \
                    .outputMode("append") \
                    .start()
                    
# Query 02
stream_query_02 = result_df.writeStream.format("console") \
                .option("checkpointLocation", "checkpoint_02") \
                .outputMode("append") \
                .start()
```

***Do not wait for any query's termination***

You should not call awaitTermination() for any one query as this will stop the application once one query terminates. Rather you should wait for the termination of the streaming context of the session.

```
spark.streams.awaitTermination()
```


## Serialization and Deserialization for Read/Write to Kafka

We have seen in all previous section about how we use JSON serialization to read and write to Kafka using Spark. We should note that though JSON is quite robust and expresive but it is not the only option. We have another test based serialization i.e. CSV and we have a binary and more efficient AVRO support also.

Using CSV is exactly similar to JSON by just replacing to_json and from_json with its CSV counter part, i.e. to_csv and from_csv.

## AVRO with Spark, Kafka Intergration

Firstly you need to set the dependency like we did for Kafka before, i choose as usual Option 3.

***Avro Sink***

```
    result_df = transformed_df.select( col("name").alias("key"),
                                       to_avro(
                                           struct("*")
                                       ).alias("value") )
				       
    stream_query = result_df.writeStream \
        .format("kafka") \
        .queryName("Kafka Writter 03") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "spark-stream-03") \
        .option("checkpointLocation", "checkpoint") \
        .outputMode("append") \
        .start()				    
```

> You need to import the Avro functions from the Avro package.

```
from pyspark.sql.avro.functions import to_avro
```

***Avro Source***

To read Avro data you need `from_avro` function but it additionally need a Avro schema to read data. Here you have two options either you give Avro schema file or provide Schema registry details.

In our example we show with a physical Avro file created with name `person.avsc` and content as below.

```
{
  "namespace": "sanmuk.avro",
  "type": "record",
  "name": "Person",
  "fields": [
    {"name": "id", "type": ["int", "null"]},
    {"name": "name", "type": ["string", "null"]},
    {"name": "country", "type": ["string", "null"]},
    {"name": "city", "type": ["string", "null"]}
  ]
}
```

> Note do not confuse Spark Dataframe schema with Avro schema these two are completely different.

Now we load the schema as string in our programe to be passed to the from_avro function.

```
    schema1 = open("person.avsc", 'r').read()
```

Now we load the data.

```
    input_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "spark-stream-03") \
        .option("startingOffsets", "earliest") \
        .load()

    output_df = input_df.select(
        from_avro(col("value"), schema1).alias("value")
    ).select( col("value.*") )
```

Now its upto you what you wanna do with the output_df dataframe.
