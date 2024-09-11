# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA sales_gold

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sales_gold.dim_orders;
# MAGIC CREATE TABLE IF NOT EXISTS sales_gold.dim_orders(
# MAGIC     Order_ID STRING COMMENT 'Identificador único do pedido.',
# MAGIC     Order_Date DATE COMMENT 'Data em que o pedido foi realizado.',
# MAGIC     Order_Status STRING COMMENT 'Status atual do pedido (ex: Shipped, Cancelled, Pending, etc...).',
# MAGIC     Fulfilment STRING COMMENT 'Método de quem realizou o envio do pedido (ex: Amazon, Merchant - Vendedor).',
# MAGIC     Sales_Channel STRING COMMENT 'Canal de venda pelo qual o pedido foi realizado (ex: Amazon.in , Non-Amazon).',
# MAGIC     B2B STRING COMMENT 'Indica se o pedido é para negócios (B2B) ou consumidor final (B2C).',
# MAGIC     Currency STRING COMMENT 'Moeda utilizada para o pagamento do pedido.'
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/mnt/datalakeeccomerceproject/prod/gold/dim_orders';

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sales_gold.dim_itens;
# MAGIC CREATE TABLE IF NOT EXISTS sales_gold.dim_itens(
# MAGIC     SKU STRING COMMENT 'Stock Keeping Unit: Identificador único do produto.',
# MAGIC     Style STRING COMMENT 'Estilo do produto.',
# MAGIC     Category STRING COMMENT 'Categoria do produto.',
# MAGIC     Size STRING COMMENT 'Tamanho do produto.',
# MAGIC     ASIN STRING COMMENT 'Amazon Standard Identification Number: Código único da Amazon.',
# MAGIC     Price DOUBLE COMMENT 'Preço do produto.'
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/mnt/datalakeeccomerceproject/prod/gold/dim_itens';

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sales_gold.dim_shipments;
# MAGIC CREATE TABLE IF NOT EXISTS sales_gold.dim_shipments(
# MAGIC     Order_ID STRING COMMENT 'Identificador único do pedido.',
# MAGIC     Ship_Status STRING COMMENT 'Tipo do envio (ex: Expedited, Standard).',
# MAGIC     Ship_City STRING COMMENT 'Cidade para a qual o pedido foi enviado.',
# MAGIC     Ship_State STRING COMMENT 'Estado para o qual o pedido foi enviado.',
# MAGIC     Ship_Postal_Code STRING COMMENT 'Código postal para a entrega do pedido.',
# MAGIC     Ship_Country STRING COMMENT 'País para o qual o pedido foi enviado.'
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/mnt/datalakeeccomerceproject/prod/gold/dim_shipments';

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sales_gold.dim_delivery;
# MAGIC CREATE TABLE IF NOT EXISTS sales_gold.dim_delivery(
# MAGIC     Order_ID STRING COMMENT 'Identificador único do pedido.',
# MAGIC     Delivery_Status STRING COMMENT 'Status da entrega do pedido.'
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/mnt/datalakeeccomerceproject/prod/gold/dim_delivery';

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sales_gold.fact_transactions;
# MAGIC CREATE TABLE IF NOT EXISTS sales_gold.fact_transactions(
# MAGIC     Transaction_ID STRING COMMENT 'Identificador único para a transação.',
# MAGIC     Order_ID STRING COMMENT 'Identificador único do pedido associado à transação.',
# MAGIC     SKU STRING COMMENT 'Identificador único do produto associado à transação.',
# MAGIC     Quantity INT COMMENT 'Quantidade de produtos vendidos na transação.',
# MAGIC     Amount DOUBLE COMMENT 'Valor total da transação (preço total dos produtos vendidos).'
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/mnt/datalakeeccomerceproject/prod/gold/fact_transactions';
