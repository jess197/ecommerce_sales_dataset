# Databricks notebook source
# MAGIC %fs ls 'dbfs:/mnt/datalakeeccomerceproject/prod/landing/'
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #Libs

# COMMAND ----------

import pyspark.sql.functions as fn
from pyspark.sql.types import StructType, StructField,StringType, IntegerType, FloatType
from delta import DeltaTable
path = "dbfs:/mnt/datalakeeccomerceproject/prod/landing/Amazon Sale Report.csv"

# COMMAND ----------

df_schema = StructType([
    StructField('index', StringType(), True),
    StructField('Order_ID', StringType(), True),
    StructField('Date', StringType(), True),
    StructField('Status', StringType(), True),
    StructField('Fulfilment', StringType(), True),
    StructField('Sales_Channel', StringType(), True),
    StructField('ship_service_level', StringType(), True),
    StructField('Style', StringType(), True),
    StructField('SKU', StringType(), True),
    StructField('Category', StringType(), True),
    StructField('Size', StringType(), True),
    StructField('ASIN', StringType(), True),
    StructField('Courier_Status', StringType(), True),
    StructField('Qty', StringType(), True),
    StructField('currency', StringType(), True),
    StructField('Amount', StringType(), True),
    StructField('ship_city', StringType(), True),
    StructField('ship_state', StringType(), True),
    StructField('ship_postal_code', StringType(), True),
    StructField('ship_country', StringType(), True),
    StructField('promotion_ids', StringType(), True),
    StructField('B2B', StringType(), True),
    StructField('fulfilled_by', StringType(), True),
    StructField('Unnamed_22', StringType(), True)  
])

# COMMAND ----------

df_amz_sales_report_landing =(
     spark
     .read
     .format("csv")
     .option("header", "true")
     .schema(df_schema)
     .load(path)
)

# COMMAND ----------

df_amz_sales_report_landing.write.format("delta").mode("overwrite").save("/mnt/datalakeeccomerceproject/prod/bronze/tb_amz_sales_report")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_bronze.tb_amz_sales_report
