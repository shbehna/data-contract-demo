# Databricks notebook source
df = spark.table("nprod.tlc.yellow_trip")

from pyspark.sql.functions import col, unix_timestamp

df = df.withColumn(
    "trip_duration_seconds",
    unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))
)
df = df.filter(col("trip_duration_seconds") > 86400)
display(df)
