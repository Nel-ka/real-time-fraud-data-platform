# Databricks notebook source
from pyspark.sql.functions import window, count, avg, countDistinct, when, approx_count_distinct

silver_stream = spark.readStream \
    .format("delta") \
    .load("/Volumes/workspace/default/silver/streaming/transactions")

# COMMAND ----------

silver_stream = silver_stream \
    .withWatermark("event_time", "1 minutes")

# COMMAND ----------

features = silver_stream.groupBy(
    "user_id",
    window("event_time", "5 minutes", "1 minute")
).agg(
    count("*").alias("txn_count_5min"),
    avg("amount").alias("avg_amount_5min"),
    approx_count_distinct("country").alias("country_count")
)

# COMMAND ----------

features = features.withColumn(
    "risk_flag",
    when(
        (features.txn_count_5min > 5) & (features.country_count > 1),
        "HIGH"
    ).otherwise("LOW")
)

# COMMAND ----------

feature_query = features.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/Volumes/workspace/default/checkpoint/streaming/gold") \
    .outputMode("append") \
    .trigger(availableNow=True) \
    .start("/Volumes/workspace/default/gold/streaming/fraud_features")