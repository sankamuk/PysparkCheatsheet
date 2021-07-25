"""
File Source and File Sink

Data:
Input Directory: ./input/file/
Output Directory: ./output/file/
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, LongType, IntegerType, DoubleType


if __name__ == '__main__' :

    spark = SparkSession \
            .builder \
            .appName("File Source and Sink") \
            .config("spark.sql.shuffle.partitions", 2) \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .getOrCreate()

    schema1 = StructType(
        [StructField('request_id', LongType()),
         StructField('name', StringType()),
         StructField('city', StringType()),
         StructField('country', StringType())])

    input_df = spark \
        .readStream \
        .format("json") \
        .option("path", "input/file/") \
        .option("cleanSource", "delete") \
        .schema(schema1) \
        .load()

    transformed_df = input_df \
        .withColumn("_country", when( col("country").isNull(), "Unknown").otherwise( col("country") ) ) \
        .drop("country") \
        .withColumnRenamed("_country", "country")

    stream_query = transformed_df.writeStream.format("json") \
        .option("checkpointLocation", "checkpoint") \
        .option("path", "output/file") \
        .outputMode("complete") \
        .trigger( processingTime="50 seconds" ) \
        .start()

    stream_query.awaitTermination()