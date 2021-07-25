"""
Stream to Static Dataframe Join

Data:
Kafka Topic: spark-stream-07
{ 'id': '1', 'login': '06-03-2021-10-24' }
{ 'id': '3', 'login': '06-03-2021-11-05' }

Cassandra DB: sankar, Table: emp
 id | last_login                      | name
----+---------------------------------+------
  3 | 2021-02-10 11:20:00.000000+0000 |  ram
  2 | 2021-02-10 14:20:00.000000+0000 |  sam
  1 | 2021-02-11 11:10:50.000000+0000 | jodu
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, from_json, to_json, struct, to_timestamp, window, max
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, LongType, IntegerType, DoubleType

def write_to_cassandra(target_df, batch_id):
    target_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "sankar") \
        .option("table", "emp") \
        .mode('append') \
        .save()

    target_df.show()


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("Stream to Static Join with Cassandra Connector") \
        .config("spark.sql.shuffle.partitions", 2) \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.executor.extraClassPath", "/Users/apple/PycharmProjects/jars/*") \
        .config("spark.driver.extraClassPath", "/Users/apple/PycharmProjects/jars/*") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0") \
        .config("spark.cassandra.connection.host", "127.0.0.1") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()

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

    sta_inp_df = spark.read\
        .format("org.apache.spark.sql.cassandra")\
        .option("keyspace", "sankar")\
        .option("table", "emp")\
        .load()

    j_expr = str_trans_df.id == sta_inp_df.id
    j_type = "inner"
    out_df = str_trans_df.join(sta_inp_df, j_expr, j_type).drop(str_trans_df.id)\
        .select(col("id"),
                col("name"),
                col("login").alias("last_login"))

    # Write back to Cassandra
    stream_query = out_df.writeStream\
        .foreachBatch(write_to_cassandra)\
        .option("checkpointLocation", "checkpoint") \
        .outputMode("update") \
        .trigger(processingTime="1 minute") \
        .start()

    stream_query.awaitTermination()