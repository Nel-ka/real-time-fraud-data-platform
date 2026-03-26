# Databricks notebook source
silver_stream = spark.readStream \
    .format("delta") \
    .load("/Volumes/workspace/default/bronze/transactions")

# COMMAND ----------

silver_stream = silver_stream.withWatermark("event_time", "10 minutes")

# COMMAND ----------

silver_clean = silver_stream.dropDuplicates(["transaction_id"])
silver_clean = silver_clean.filter(
    "transaction_id IS NOT NULL AND user_id IS NOT NULL AND amount IS NOT NULL"
)

# COMMAND ----------

silver_query = silver_clean.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/Volumes/workspace/default/checkpoint/silver") \
    .outputMode("append") \
    .trigger(availableNow=True) \
    .start("/Volumes/workspace/default/silver/streaming/transactions")