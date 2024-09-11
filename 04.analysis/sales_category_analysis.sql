
 ### Análise de Vendas por Categoria
 - Objetivo: Entender a performance de diferentes categorias de produtos.

 SELECT 
     it.Category, 
     SUM(ft.Quantity) AS Total_Quantity_Sold, 
     ROUND(SUM(ft.Amount),2) AS Total_Revenue,
     ROUND((Total_Revenue/83.9713),2) as Total_Revenue_Dolar
 FROM 
     sales_gold.fact_transactions ft
 JOIN 
     sales_gold.dim_itens it ON ft.SKU = it.SKU
 WHERE ft.Quantity > 0
 GROUP BY 
     it.Category
 ORDER BY 
     Total_Revenue DESC;