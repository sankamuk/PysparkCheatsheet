"""
Tumbling Window with Watermark

Data:
Kafka Topic: spark-stream-05
{ 'id': 'apple', 'amount': 10, 'type': 'buy', 'time': '20-04-2020-10-04' }
{ 'id': 'apple', 'amount': 5, 'type': 'sell', 'time': '20-04-2020-10-06' }
{ 'id': 'dell', 'amount': 8, 'type': 'sell', 'time': '20-04-2020-10-11' }
{ 'id': 'shell', 'amount': 18, 'type': 'buy', 'time': '20-04-2020-10-22' }
{ 'id': 'wallmart', 'amount': 19, 'type': 'buy', 'time': '20-04-2020-10-24' }

"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, from_json, to_json, struct, to_timestamp, window, sum
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, LongType, IntegerType, DoubleType

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("Watermarked Tumbling Window") \
        .config("spark.sql.shuffle.partitions", 2) \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.executor.extraClassPath", "/Users/apple/PycharmProjects/jars/*") \
        .config("spark.driver.extraClassPath", "/Users/apple/PycharmProjects/jars/*") \
        .getOrCreate()

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

    #transformed_df.printSchema()
    agg_df = transformed_df\
    .withWatermark('eventtime', '15 minutes')\
    .groupby(
        window(
            'eventtime', '5 minutes'
        )
    ).agg( sum('profit').alias('profit') )


    stream_query = agg_df.writeStream.format("console") \
        .option("checkpointLocation", "checkpoint") \
        .outputMode("append") \
        .option("truncate", "false") \
        .start()

    stream_query.awaitTermination()
