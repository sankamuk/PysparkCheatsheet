"""
Kafka Avro Source

Data:

"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.avro.functions import to_avro, from_avro

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("Kafka Avro Source") \
        .config("spark.sql.shuffle.partitions", 2) \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.executor.extraClassPath", "/Users/apple/PycharmProjects/jars/*") \
        .config("spark.driver.extraClassPath", "/Users/apple/PycharmProjects/jars/*") \
        .getOrCreate()

    schema1 = open("person.avsc", 'r').read()

    input_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "spark-stream-03") \
        .option("startingOffsets", "earliest") \
        .load()

    output_df = input_df.select(
        from_avro(col("value"), schema1).alias("value")
        ).select(
        col("value.*")
        )

    output_df.show()
