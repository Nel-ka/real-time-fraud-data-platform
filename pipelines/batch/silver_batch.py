# Databricks notebook source
from pyspark.sql.functions import upper

# COMMAND ----------

silver_df = spark.read.format("delta") \
    .load("/Volumes/workspace/default/bronze/transactions")
silver_df = silver_df.dropDuplicates(["transaction_id"])
# silver_df.display()

# COMMAND ----------

silver_df = silver_df.filter(
    "transaction_id IS NOT NULL AND user_id IS NOT NULL AND amount IS NOT NULL"
)
silver_df = silver_df.withColumn("amount", silver_df["amount"].cast("double"))

# COMMAND ----------

silver_df = silver_df.withColumn("country", upper("country"))

# COMMAND ----------

silver_df = silver_df.select(
    "transaction_id",
    "user_id",
    "merchant_id",
    "amount",
    "currency",
    "country",
    "device_id",
    "event_time",
    "ingestion_time"
)

# COMMAND ----------

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/Volumes/workspace/default/silver/transactions")