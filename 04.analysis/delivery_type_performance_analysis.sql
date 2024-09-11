 ### Análise de Desempenho por Tipo de Entrega por Estado
 - Objetivo: Avaliar qual a preferência de tipo de entrega escolhido pelos clientes por estado

 WITH Status_Counts AS (
     SELECT 
         s.Ship_State,
         s.Ship_Status,
         COUNT(*) AS Status_Count
     FROM 
         sales_gold.dim_shipments s
     GROUP BY 
         Ship_State, Ship_Status
 ),
 Pivoted_Status AS (
     SELECT 
         Ship_State,
         MAX(CASE WHEN Ship_Status = 'Expedited' THEN Status_Count ELSE 0 END) AS Expedited,
         MAX(CASE WHEN Ship_Status = 'Standard' THEN Status_Count ELSE 0 END) AS Standard
     FROM 
         Status_Counts
     GROUP BY 
         Ship_State
 )
 SELECT 
     Ship_State,
     Expedited,
     Standard,
     CASE 
         WHEN Expedited > Standard THEN 'Expedited'
         ELSE 'Standard'
     END AS More_Common_Status
 FROM 
     Pivoted_Status
 ORDER BY Expedited desc;
