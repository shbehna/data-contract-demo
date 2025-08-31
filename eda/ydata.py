# Databricks notebook source
# DBTITLE 1,Imports
global spark, dbutils

# Databricks notebook source
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

    profile.to_file(f"/tmp/{table_name}.html")
    dbutils.fs.cp(f"file:/tmp/{table_name}.html", f"dbfs:/Volumes/dev_migration_brute/eda/fadq_oracle/ydata_reports/{table_name}.html")

# COMMAND ----------

process_table("yellow_trip")