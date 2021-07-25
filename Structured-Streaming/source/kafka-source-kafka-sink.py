"""
Kafka JSon Source and Sink

Data:
Kafka Topic: spark-stream-01
{ "id": 1, "name": "Sankar", "city": "kolkata", "country": "india" }
{ "id": 1, "name": "Kun", "city": "tiangen" }

"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, from_json, to_json
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, LongType, IntegerType, DoubleType


if __name__ == '__main__' :

    spark = SparkSession \
            .builder \
            .appName("Kafka Source and Sink") \
            .config("spark.sql.shuffle.partitions", 2) \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.executor.extraClassPath", "/Users/apple/PycharmProjects/jars/*") \
            .config("spark.driver.extraClassPath", "/Users/apple/PycharmProjects/jars/*") \
            .getOrCreate()

    schema1 = StructType(
        [StructField('id', LongType()), StructField('name', StringType()), StructField('city', StringType()),
         StructField('country', StringType())])

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

    # result_df.show()
    stream_query = result_df.writeStream \
        .format("kafka") \
        .queryName("Kafka Writter 02") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "spark-stream-02") \
        .option("checkpointLocation", "checkpoint") \
        .outputMode("append") \
        .start()


    stream_query.awaitTermination()