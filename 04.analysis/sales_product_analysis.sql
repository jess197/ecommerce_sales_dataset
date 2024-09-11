### Análise de Vendas por Produto
- Objetivo: Identificar quais produtos estão vendendo mais e quais têm maior receita.

with CTE_Total_Quantity_Amount_Sold as (
SELECT 
    ft.SKU, 
    it.Style, 
    it.Category, 
    SUM(ft.Quantity) AS Total_Quantity_Sold,
    SUM(ft.Amount) AS Total_Revenue
FROM 
    sales_gold.fact_transactions ft
JOIN 
    sales_gold.dim_itens it ON ft.SKU = it.SKU
WHERE ft.Quantity > 0
GROUP BY 
    ft.SKU, it.Style, it.Category
ORDER BY 
    Total_Revenue DESC
)
SELECT 
   SKU
   ,Style
   ,Category
   ,Total_Quantity_Sold
   ,ROUND(CAST(Total_Revenue AS DECIMAL(18, 2)),2) AS Total_Revenue
   ,ROUND((Total_Revenue/83.9713),2) as Total_Revenue_Dolar
  FROM CTE_Total_Quantity_Amount_Sold
