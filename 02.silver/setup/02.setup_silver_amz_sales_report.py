# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA sales_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sales_silver.tb_amz_sales_report;
# MAGIC CREATE TABLE IF NOT EXISTS sales_silver.tb_amz_sales_report
# MAGIC (
# MAGIC   `id` string,
# MAGIC   `Order_ID` string,
# MAGIC   `Order_Date` date,
# MAGIC   `Order_Status` string,
# MAGIC   `Fulfilment` string,
# MAGIC   `Sales_Channel` string,
# MAGIC   `Ship_Status` string,
# MAGIC   `Style` string,
# MAGIC   `SKU` string,
# MAGIC   `Category` string,
# MAGIC   `Size` string,
# MAGIC   `ASIN` string,
# MAGIC   `Delivery_Status` string,
# MAGIC   `Quantity` int,
# MAGIC   `Currency` string,
# MAGIC   `Amount` double,
# MAGIC   `Ship_City` string,
# MAGIC   `Ship_State` string,
# MAGIC   `Ship_Postal_Code` string,
# MAGIC   `Ship_Country` string,
# MAGIC   `B2B` string,
# MAGIC   `Price` double
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/mnt/datalakeeccomerceproject/prod/silver/tb_amz_sales_report';
# MAGIC
