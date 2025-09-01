# Databricks notebook source
# DBTITLE 1,Imports
global spark, dbutils

from ydata_profiling import ProfileReport
from pyspark.sql.functions import col, when, unix_timestamp, lit
from pyspark.sql.types import DecimalType, FloatType
from concurrent.futures import ThreadPoolExecutor

# COMMAND ----------

# DBTITLE 1,Constants
catalog = "nprod"
schema = "tlc"

# COMMAND ----------

# DBTITLE 1,Processing function
def process_table(table_name):
    df = spark.read.format("delta").table(f"{catalog}.{schema}.{table_name}")
    df = df.sample(fraction=0.1)

    df_koalas = df.pandas_api()

    profile = ProfileReport(df_koalas.to_pandas(), 
                            title=f"EDA - {table_name}"
                            )
    
    profile.config.correlations["auto"].calculate = False
    profile.config.correlations["pearson"].calculate = False
    profile.config.correlations["spearman"].calculate = False
    profile.config.correlations["kendall"].calculate = False
    profile.config.correlations["phi_k"].calculate = False
    profile.config.correlations["cramers"].calculate = False

    profile.config.interactions.continuous = False
    profile.config.interactions.targets = []

    return profile

# COMMAND ----------

profile = process_table("yellow_trip")

# COMMAND ----------

profile.to_file(f"/Volumes/nprod/tlc/ydata/yellow_trip.html")

# COMMAND ----------


