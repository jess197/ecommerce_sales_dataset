# Databricks notebook source
# MAGIC %md
# MAGIC # Libs

# COMMAND ----------

import pyspark.sql.functions as fn 
from pyspark.sql.types import IntegerType, DoubleType, DecimalType

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC   FROM sales_bronze.tb_amz_sales_report

# COMMAND ----------

df_amz_sales_report_silver = (
    table('sales_bronze.tb_amz_sales_report')
    .select(fn.col('index').alias('id'),
            fn.col('Order_ID'),
            fn.to_date(fn.col('date'),'MM-dd-yy').alias('Order_Date'),
            fn.col('Status').alias('Order_Status'),
            fn.col('Fulfilment'),
            fn.col('Sales_Channel'),
            fn.col('ship_service_level').alias('Ship_Status'),
            fn.col('Style'),
            fn.col('SKU'),
            fn.upper(fn.col('Category')).alias('Category'),
            fn.col('Size'),
            fn.col('ASIN'),
            fn.coalesce(fn.col('Courier_Status'),fn.lit('N/A')).alias('Delivery_Status'),
            fn.col('Qty').alias('Quantity').cast(IntegerType()),
            fn.coalesce(fn.col('Currency'),fn.lit('N/A')).alias('Currency'),
            fn.coalesce(fn.col('Amount'),fn.lit(0)).cast(DoubleType()).alias('Amount'),
            fn.coalesce(fn.upper(fn.col('Ship_City')),fn.lit('N/A')).alias('Ship_City'),
            fn.coalesce(fn.upper(fn.col('Ship_State')),fn.lit('N/A')).alias('Ship_State'),
            fn.coalesce(fn.col('Ship_Postal_Code'),fn.lit('N/A')).alias('Ship_Postal_Code'),
            fn.coalesce(fn.col('Ship_Country'),fn.lit('N/A')).alias('Ship_Country'),
            fn.col('B2B')
            ).withColumn('Price',(fn.coalesce(fn.col('Amount'), fn.lit(0)) / fn.coalesce(fn.col('Quantity'), fn.lit(1)))  # Avoid division by zero
            )
    )


# COMMAND ----------

df_amz_sales_report_silver_treated = (
    df_amz_sales_report_silver
    .withColumn('Price', fn.coalesce(fn.col('Price'), fn.lit(0)))
)

# COMMAND ----------

df_amz_sales_report_silver_treated.write.format("delta").mode("overwrite").save("/mnt/datalakeeccomerceproject/prod/silver/tb_amz_sales_report")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/mnt/datalakeeccomerceproject/prod/silver/tb_amz_sales_report`
