### Análise de Performance de Entregas
- Objetivo: Monitorar e melhorar o desempenho das entregas.

%sql
SELECT 
    s.Ship_Country, 
    s.Ship_State, 
    s.Ship_Status, 
    COUNT(ft.Order_ID) AS Total_Shipments 
FROM 
    sales_gold.fact_transactions ft
JOIN 
    sales_gold.dim_shipments s ON ft.Order_ID = s.Order_ID
WHERE s.Ship_Country <> 'N/A'
GROUP BY 
    s.Ship_Country, s.Ship_State, s.Ship_Status
ORDER BY 
    Total_Shipments DESC;
