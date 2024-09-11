### Análise de Pedidos B2B vs. B2C
- Objetivo: Comparar o desempenho das vendas entre clientes B2B e B2C.

SELECT 
    CASE WHEN (or.B2B = 'False') THEN 'B2C' ELSE 'B2B' END AS B2B_BC2, 
    COUNT(DISTINCT ft.Order_ID) AS Total_Orders, 
    ROUND(SUM(ft.Amount),2) AS Total_Revenue,
    ROUND((Total_Revenue/83.9713),2) as Total_Revenue_Dolar
FROM 
    sales_gold.fact_transactions ft
JOIN 
    sales_gold.dim_orders or ON ft.Order_ID = or.Order_ID
WHERE ft.Quantity > 0
GROUP BY 
    or.B2B
ORDER BY 
    Total_Revenue DESC;