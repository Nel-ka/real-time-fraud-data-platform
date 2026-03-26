# Databricks notebook source
df = spark.read.json("/Volumes/workspace/default/stream/transactions_0.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name, col, to_timestamp

df = df.withColumn("event_time", to_timestamp(col("timestamp")))

bronze_df = df.withColumn("ingestion_time", current_timestamp()) \
    .withColumn("source_file", col("_metadata.file_path"))

# COMMAND ----------

bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/Volumes/workspace/default/bronze/transactions")