# Databricks notebook source
# MAGIC %sql
# MAGIC select * from sales_gold.dim_orders

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise de Vendas por Produto
# MAGIC - Objetivo: Identificar quais produtos estão vendendo mais e quais têm maior receita.

# COMMAND ----------

# MAGIC %sql
# MAGIC with CTE_Total_Quantity_Amount_Sold as (
# MAGIC SELECT 
# MAGIC     ft.SKU, 
# MAGIC     it.Style, 
# MAGIC     it.Category, 
# MAGIC     SUM(ft.Quantity) AS Total_Quantity_Sold,
# MAGIC     SUM(ft.Amount) AS Total_Revenue
# MAGIC FROM 
# MAGIC     sales_gold.fact_transactions ft
# MAGIC JOIN 
# MAGIC     sales_gold.dim_itens it ON ft.SKU = it.SKU
# MAGIC WHERE ft.Quantity > 0
# MAGIC GROUP BY 
# MAGIC     ft.SKU, it.Style, it.Category
# MAGIC ORDER BY 
# MAGIC     Total_Revenue DESC
# MAGIC )
# MAGIC SELECT 
# MAGIC    SKU
# MAGIC    ,Style
# MAGIC    ,Category
# MAGIC    ,Total_Quantity_Sold
# MAGIC    ,ROUND(CAST(Total_Revenue AS DECIMAL(18, 2)),2) AS Total_Revenue
# MAGIC    ,ROUND((Total_Revenue/83.9713),2) as Total_Revenue_Dolar
# MAGIC   FROM CTE_Total_Quantity_Amount_Sold

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise de Vendas por Categoria
# MAGIC - Objetivo: Entender a performance de diferentes categorias de produtos.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     it.Category, 
# MAGIC     SUM(ft.Quantity) AS Total_Quantity_Sold, 
# MAGIC     ROUND(SUM(ft.Amount),2) AS Total_Revenue,
# MAGIC     ROUND((Total_Revenue/83.9713),2) as Total_Revenue_Dolar
# MAGIC FROM 
# MAGIC     sales_gold.fact_transactions ft
# MAGIC JOIN 
# MAGIC     sales_gold.dim_itens it ON ft.SKU = it.SKU
# MAGIC WHERE ft.Quantity > 0
# MAGIC GROUP BY 
# MAGIC     it.Category
# MAGIC ORDER BY 
# MAGIC     Total_Revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise de Performance de Entregas
# MAGIC - Objetivo: Monitorar e melhorar o desempenho das entregas.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     s.Ship_Country, 
# MAGIC     s.Ship_State, 
# MAGIC     s.Ship_Status, 
# MAGIC     COUNT(ft.Order_ID) AS Total_Shipments 
# MAGIC FROM 
# MAGIC     sales_gold.fact_transactions ft
# MAGIC JOIN 
# MAGIC     sales_gold.dim_shipments s ON ft.Order_ID = s.Order_ID
# MAGIC WHERE s.Ship_Country <> 'N/A'
# MAGIC GROUP BY 
# MAGIC     s.Ship_Country, s.Ship_State, s.Ship_Status
# MAGIC ORDER BY 
# MAGIC     Total_Shipments DESC;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise de Pedidos B2B vs. B2C
# MAGIC - Objetivo: Comparar o desempenho das vendas entre clientes B2B e B2C.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     CASE WHEN (or.B2B = 'False') THEN 'B2C' ELSE 'B2B' END AS B2B_BC2, 
# MAGIC     COUNT(DISTINCT ft.Order_ID) AS Total_Orders, 
# MAGIC     ROUND(SUM(ft.Amount),2) AS Total_Revenue,
# MAGIC     ROUND((Total_Revenue/83.9713),2) as Total_Revenue_Dolar
# MAGIC FROM 
# MAGIC     sales_gold.fact_transactions ft
# MAGIC JOIN 
# MAGIC     sales_gold.dim_orders or ON ft.Order_ID = or.Order_ID
# MAGIC WHERE ft.Quantity > 0
# MAGIC GROUP BY 
# MAGIC     or.B2B
# MAGIC ORDER BY 
# MAGIC     Total_Revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise de Desempenho por Estado
# MAGIC - Objetivo: Avaliar a performance das vendas por localização geográfica.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     s.Ship_State, 
# MAGIC     COUNT(ft.Order_ID) AS Total_Orders, 
# MAGIC     ROUND(SUM(ft.Amount),2) AS Total_Revenue 
# MAGIC FROM 
# MAGIC     sales_gold.fact_transactions ft
# MAGIC JOIN 
# MAGIC     sales_gold.dim_shipments s ON ft.Order_ID = s.Order_ID
# MAGIC WHERE ft.Quantity > 0
# MAGIC GROUP BY 
# MAGIC s.Ship_State
# MAGIC ORDER BY 
# MAGIC     Total_Revenue DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise de Desempenho por Tipo de Entrega por Estado
# MAGIC - Objetivo: Avaliar qual a preferência de tipo de entrega escolhido pelos clientes por estado

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH Status_Counts AS (
# MAGIC     SELECT 
# MAGIC         s.Ship_State,
# MAGIC         s.Ship_Status,
# MAGIC         COUNT(*) AS Status_Count
# MAGIC     FROM 
# MAGIC         sales_gold.dim_shipments s
# MAGIC     GROUP BY 
# MAGIC         Ship_State, Ship_Status
# MAGIC ),
# MAGIC Pivoted_Status AS (
# MAGIC     SELECT 
# MAGIC         Ship_State,
# MAGIC         MAX(CASE WHEN Ship_Status = 'Expedited' THEN Status_Count ELSE 0 END) AS Expedited,
# MAGIC         MAX(CASE WHEN Ship_Status = 'Standard' THEN Status_Count ELSE 0 END) AS Standard
# MAGIC     FROM 
# MAGIC         Status_Counts
# MAGIC     GROUP BY 
# MAGIC         Ship_State
# MAGIC )
# MAGIC SELECT 
# MAGIC     Ship_State,
# MAGIC     Expedited,
# MAGIC     Standard,
# MAGIC     CASE 
# MAGIC         WHEN Expedited > Standard THEN 'Expedited'
# MAGIC         ELSE 'Standard'
# MAGIC     END AS More_Common_Status
# MAGIC FROM 
# MAGIC     Pivoted_Status
# MAGIC ORDER BY Expedited desc;
# MAGIC
