# Databricks notebook source
from pyspark.sql.functions import col, to_date, count, when

# COMMAND ----------



# Load the nprod.tlc.yellow_trip table
yellow_trip_df = spark.table("nprod.tlc.yellow_trip")

# Select the required columns
fact_trip_df = yellow_trip_df.select(
    col("tpep_pickup_datetime").alias("pickup_datetime"),
    col("tpep_dropoff_datetime").alias("dropoff_datetime"),
    "passenger_count",
    "trip_distance",
    "fare_amount",
    col("PULocationID").alias("pickup_location_id"),
    col("DOLocationID").alias("dropoff_location_id")
)

fact_trip_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("nprod.tlc.fact_trip")

# COMMAND ----------

# Load the nprod.tlc.zone table
zone_df = spark.table("nprod.tlc.zone")

# Write the DataFrame to a new table dim_zone
zone_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("nprod.tlc.dim_zone")

# COMMAND ----------

# Load the nprod.tlc.yellow_trip table
yellow_trip_df = spark.table("nprod.tlc.yellow_trip_valid")

# Extract the pickup date
yellow_trip_df = yellow_trip_df.withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))

# Summarize the data per day
from pyspark.sql.functions import sequence, lit, current_date

# Generate a DataFrame with all dates from the earliest pickup_date to the current date

date_range_df = spark.range(0, 1).selectExpr("sequence('2025-01-01', current_date(), interval 1 day) as all_dates").selectExpr("explode(all_dates) as pickup_date")

# Summarize the data per day
summary_df = date_range_df.join(yellow_trip_df.groupBy("pickup_date").agg(
    count(when(col("_errors").isNotNull(), True)).alias("error_count"),
    count(when(col("_errors").isNull() & col("_warnings").isNotNull(), True)).alias("warning_count"),
    count(when(col("_errors").isNull() & col("_warnings").isNull(), True)).alias("no_issues_count")
), on="pickup_date", how="left")

# Create a table with the summary
summary_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("nprod.tlc.daily_summary")

# COMMAND ----------


