# Databricks notebook source
df = spark.read.format("csv").option("header", True).load("/Volumes/nprod/tlc/tlc_lookup/taxi_zone_lookup.csv")
df.write.saveAsTable("nprod.tlc.zone")
