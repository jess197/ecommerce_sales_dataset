# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA sales_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sales_bronze.tb_amz_sales_report;
# MAGIC CREATE TABLE IF NOT EXISTS sales_bronze.tb_amz_sales_report
# MAGIC (
# MAGIC   `index` string,
# MAGIC   `Order_ID` string,
# MAGIC   `Date` string,
# MAGIC   `Status` string,
# MAGIC   `Fulfilment` string,
# MAGIC   `Sales_Channel` string,
# MAGIC   `ship_service_level` string,
# MAGIC   `Style` string,
# MAGIC   `SKU` string,
# MAGIC   `Category` string,
# MAGIC   `Size` string,
# MAGIC   `ASIN` string,
# MAGIC   `Courier_Status` string,
# MAGIC   `Qty` string,
# MAGIC   `currency` string,
# MAGIC   `Amount` string,
# MAGIC   `ship_city` string,
# MAGIC   `ship_state` string,
# MAGIC   `ship_postal_code` string,
# MAGIC   `ship_country` string,
# MAGIC   `promotion_ids` string,
# MAGIC   `B2B` string,
# MAGIC   `fulfilled_by` string,
# MAGIC   `Unnamed_22` string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/mnt/datalakeeccomerceproject/prod/bronze/tb_amz_sales_report'
