"""
Stream to Stream Join Example

Data:
Kafka Topic: spark-stream-001
{ 'id': '22', 'type': 'linux', 'service': 'mysql', 'start_time': '06-03-2021-10-10' }
{ 'id': '34', 'type': 'linux', 'service': 'postgre', 'start_time': '06-03-2021-10-25' }
{ 'id': '44', 'type': 'windows', 'service': 'sqlserver', 'start_time': '06-03-2021-10-33' }

Kafka Topic: spark-stream-002
{ 'id': '1', 'user': 'ram', 'login': '06-03-2021-10-15', 'host_id': '22' }
{ 'id': '12', 'user': 'sam', 'login': '06-03-2021-10-24', 'host_id': '22' }
{ 'id': '1', 'user': 'ram', 'login': '06-03-2021-10-30', 'host_id': '34' }
{ 'id': '12', 'user': 'sam', 'login': '06-03-2021-10-40', 'host_id': '22' }
{ 'id': '8', 'user': 'jodu', 'login': '06-03-2021-10-50', 'host_id': '44' }
{ 'id': '9', 'user': 'modhu', 'login': '06-03-2021-10-50', 'host_id': '44' }
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, from_json, to_json, struct, to_timestamp, window, max, expr
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, LongType, IntegerType, DoubleType

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("Stream to Stream Join") \
        .config("spark.sql.shuffle.partitions", 2) \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.executor.extraClassPath", "/Users/apple/PycharmProjects/jars/*") \
        .config("spark.driver.extraClassPath", "/Users/apple/PycharmProjects/jars/*") \
        .getOrCreate()

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
        .withWatermark('start_time', '30 minute')

    trans_df_02 = str_inp_01.select(col("value").cast("string")) \
        .withColumn("value_json", from_json(col("value"), schema02)) \
        .select("value_json.*").withColumn( 'login', to_timestamp( col('login'), 'dd-MM-yyyy-HH-mm' ))\
        .withColumnRenamed('id', 'user_id')\
        .withWatermark('login', '30 minute')

    #j_expr = trans_df_01.id == trans_df_02.host_id
    j_expr = 'id == host_id'
    j_type = 'inner'
    join_df = trans_df_01.join(trans_df_02, expr(j_expr), j_type)

    stream_query = join_df.writeStream.format("console") \
        .option("checkpointLocation", "checkpoint") \
        .outputMode("append") \
        .option("truncate", "false") \
        .trigger(processingTime="1 minute") \
        .start()

    stream_query.awaitTermination()