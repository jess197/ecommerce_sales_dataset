import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
from pyspark.sql import SparkSession
from databricks import sql
import os
import seaborn as sns

# Inicializando SparkSession
#spark = SparkSession.builder.appName("StreamlitApp").getOrCreate()

# Executando a consulta SQL no Spark
BASE_QUERY = """
SELECT 
    CASE WHEN (or.B2B = 'False') THEN 'B2C' ELSE 'B2B' END AS B2B_BC2, 
    COUNT(DISTINCT ft.Order_ID) AS Total_Orders, 
    ROUND(SUM(ft.Amount),2) AS Total_Revenue,
    ROUND((SUM(ft.Amount)/83.9713),2) as Total_Revenue_Dolar
FROM 
    sales_gold.fact_transactions ft
JOIN 
    sales_gold.dim_orders or ON ft.Order_ID = or.Order_ID
WHERE ft.Quantity > 0
GROUP BY 
    or.B2B
ORDER BY 
    Total_Revenue DESC;
"""


class Databricks:

    def __init__(self):
        self.connection = sql.connect(
            server_hostname= "adb-3071876298786017.17.azuredatabricks.net",#os.environ['server_hostname'],
            http_path= "sql/protocolv1/o/3071876298786017/0909-012602-u5afvl2p",#os.environ['http_path'],
            access_token="dapi8866332dff6118b9e83c46c5b20bd82b-3"#s.environ['access_token'],
        )

    def get_result(self):
        return pd.read_sql(BASE_QUERY, self.connection)
    
dtbricks = Databricks()

df = dtbricks.get_result()

# Configurações do Streamlit
st.set_page_config(page_title="Análise de Pedidos B2B vs. B2C")

# Título da página
st.title("Análise de Pedidos B2B vs. B2C")

# Exibe os dados
st.write("Dados de Vendas B2B vs. B2C:", df)

# Criação do gráfico
fig, ax = plt.subplots(figsize=(6, 4))

# Definindo a paleta de cores
colors = sns.color_palette("pastel")[0:2]

# Criando o gráfico de barras
bars = ax.bar(df['B2B_BC2'], df['Total_Revenue_Dolar'], color=colors)

# Adicionando rótulos de texto nas barras
for bar in bars:
    yval = bar.get_height()
    ax.text(
        bar.get_x() + bar.get_width() / 2.0, 
        yval + 0.01 * yval, 
        f"${yval:,.2f}", 
        ha='center', 
        va='bottom',
        fontsize=8
    )

# Configurações do gráfico
ax.set_title('Comparação de Vendas: B2B vs. B2C', fontsize=12)  
ax.set_xlabel('Tipo de Cliente', fontsize=10)
ax.set_ylabel('Total de Receita (USD)', fontsize=10)
ax.set_xticks(df['B2B_BC2'])
ax.set_xticklabels(df['B2B_BC2'], rotation=45, ha='right')

# Ajuste no layout para evitar sobreposição
plt.tight_layout()

# Exibindo o gráfico no Streamlit
st.pyplot(fig)

