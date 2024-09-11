# Databricks notebook source
# MAGIC %md
# MAGIC # Libs

# COMMAND ----------

import pyspark.sql.functions as fn

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC   FROM sales_silver.tb_amz_sales_report

# COMMAND ----------

df_dim_orders = (
    table('sales_silver.tb_amz_sales_report')
    .select(
        fn.col('Order_ID'),
        fn.col('Order_Date'),
        fn.col('Order_Status'),
        fn.col('Fulfilment'),
        fn.col('Sales_Channel'),
        fn.col('B2B'),
        fn.col('Currency')
    )
)

# COMMAND ----------

df_dim_itens = (
    table('sales_silver.tb_amz_sales_report')
    .select(
        fn.col('SKU'),
        fn.col('Style'),
        fn.col('Category'),
        fn.col('Size'),
        fn.col('ASIN'),
        fn.col('Price')
        )
    )

# COMMAND ----------

df_dim_shipment = (
    table('sales_silver.tb_amz_sales_report')
    .select(
        fn.col('Order_ID'),
        fn.col('Ship_Status'),
        fn.col('Ship_City'),
        fn.col('Ship_State'),
        fn.col('Ship_Postal_Code'),
        fn.col('Ship_Country')
    )
)

# COMMAND ----------

df_dim_delivery = (
    table('sales_silver.tb_amz_sales_report')
    .select(
        fn.col('Order_ID'),
        fn.col('Delivery_Status')
    )
)

# COMMAND ----------

df_fact_transactions = (
    table('sales_silver.tb_amz_sales_report')
    .select(
        fn.col('id').alias('Transaction_ID'),
        fn.col('Order_ID'),
        fn.col('SKU'),
        fn.col('Quantity'),
        fn.col('Amount')
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### INSERT DELTA

# COMMAND ----------

df_dim_orders.write.format("delta").mode("overwrite").save("/mnt/datalakeeccomerceproject/prod/gold/dim_orders")

# COMMAND ----------

df_dim_itens.write.format("delta").mode("overwrite").save("/mnt/datalakeeccomerceproject/prod/gold/dim_itens")

# COMMAND ----------

df_dim_shipment.write.format("delta").mode("overwrite").save("/mnt/datalakeeccomerceproject/prod/gold/dim_shipments")

# COMMAND ----------

df_dim_delivery.write.format("delta").mode("overwrite").save("/mnt/datalakeeccomerceproject/prod/gold/dim_delivery")

# COMMAND ----------

df_fact_transactions.write.format("delta").mode("overwrite").save("/mnt/datalakeeccomerceproject/prod/gold/fact_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### INSERT PARQUET

# COMMAND ----------

df_dim_orders.write.format("parquet").mode("overwrite").save("/mnt/datalakeeccomerceproject/prod/gold/parquet/dim_orders")

# COMMAND ----------

df_dim_itens.write.format("parquet").mode("overwrite").save("/mnt/datalakeeccomerceproject/prod/gold/parquet/dim_itens")

# COMMAND ----------

df_dim_shipment.write.format("parquet").mode("overwrite").save("/mnt/datalakeeccomerceproject/prod/gold/parquet/dim_shipments")

# COMMAND ----------

df_dim_delivery.write.format("parquet").mode("overwrite").save("/mnt/datalakeeccomerceproject/prod/gold/parquet/dim_delivery")

# COMMAND ----------

df_fact_transactions.write.format("parquet").mode("overwrite").save("/mnt/datalakeeccomerceproject/prod/gold/parquet/fact_transactions")
