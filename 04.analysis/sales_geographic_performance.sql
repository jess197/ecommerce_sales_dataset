### Análise de Desempenho por Estado
- Objetivo: Avaliar a performance das vendas por localização geográfica.
SELECT 
    s.Ship_State, 
    COUNT(ft.Order_ID) AS Total_Orders, 
    ROUND(SUM(ft.Amount),2) AS Total_Revenue 
FROM 
    sales_gold.fact_transactions ft
JOIN 
    sales_gold.dim_shipments s ON ft.Order_ID = s.Order_ID
WHERE ft.Quantity > 0
GROUP BY 
s.Ship_State
ORDER BY 
    Total_Revenue DESC;
