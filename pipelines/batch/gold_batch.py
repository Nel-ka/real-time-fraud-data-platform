# Databricks notebook source
from pyspark.sql.functions import window, count, avg, countDistinct, when

# COMMAND ----------

df = spark.read.format("delta") \
    .load("/Volumes/workspace/default/silver/transactions")

# COMMAND ----------

feature_df = df.groupBy(
    "user_id",
    window("event_time", "5 minutes", "1 minute")
).agg(
    count("*").alias("txn_count_5min"),
    avg("amount").alias("avg_amount_5min")
)

# COMMAND ----------


country_df = df.groupBy(
    "user_id",
    window("event_time", "5 minutes")
).agg(
    countDistinct("country").alias("country_count")
)

# COMMAND ----------

final_features = feature_df.join(
    country_df,
    ["user_id", "window"]
)

# COMMAND ----------

final_features = final_features.withColumn(
    "risk_flag",
    when(
        (final_features.txn_count_5min > 5) & 
        (final_features.country_count > 1),
        "HIGH"
    ).otherwise("LOW")
)

# COMMAND ----------

final_features = final_features \
    .withColumn("window_start", final_features.window.start) \
    .withColumn("window_end", final_features.window.end) \
    .drop("window")

# COMMAND ----------

final_features.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/Volumes/workspace/default/gold/fraud_features")

# COMMAND ----------

features = spark.read.format("delta") \
    .load("/Volumes/workspace/default/gold/fraud_features")