# Databricks notebook source
from pyspark.sql.functions import current_timestamp, input_file_name, col, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType

schema = StructType() \
    .add("amount", DoubleType()) \
    .add("country", StringType()) \
    .add("currency", StringType()) \
    .add("device_id", StringType()) \
    .add("merchant_id", StringType()) \
    .add("timestamp", StringType()) \
    .add("transaction_id", StringType()) \
    .add("user_id", StringType())

BASE_PATH = "/Volumes/workspace/default"
bronze_stream = spark.readStream \
    .format("json") \
    .schema(schema) \
    .option("maxFilesPerTrigger", 1) \
    .load(f"{BASE_PATH}/stream/")

# COMMAND ----------

bronze_stream = bronze_stream \
    .withColumn("event_time", to_timestamp(col("timestamp"))) \
    .withColumn("ingestion_time", current_timestamp()) \
    .withColumn("source_file",  col("_metadata.file_path"))

# COMMAND ----------

bronze_query = bronze_stream.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/Volumes/workspace/default/checkpoint/bronze") \
    .outputMode("append") \
    .trigger(availableNow=True) \
    .start("/Volumes/workspace/default/bronze/transactions")