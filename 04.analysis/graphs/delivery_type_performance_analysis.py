import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
from pyspark.sql import SparkSession
from databricks import sql
import os
import seaborn as sns

# Executando a consulta SQL no Spark
BASE_QUERY = """
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
ORDER BY (Expedited + Standard) DESC
LIMIT 10;
"""


class Databricks:

    def __init__(self):
        self.connection = sql.connect(
            server_hostname= os.environ['server_hostname'],
            http_path= os.environ['http_path'],
            access_token=os.environ['access_token'],
        )

    def get_result(self):
        return pd.read_sql(BASE_QUERY, self.connection)
    
dtbricks = Databricks()

df = dtbricks.get_result()

# Configurações do Streamlit
st.set_page_config(page_title="Análise de Desempenho por Tipo de Entrega")

# Título da página
st.title("Análise de Desempenho por Tipo de Entrega por Estado")

# Exibe os dados
st.write("Dados de Desempenho por Tipo de Entrega por Estado:", df)

# Criação do gráfico
fig, ax = plt.subplots(figsize=(12, 7))

# Configurações do gráfico
plt.style.use('ggplot')  # Define um estilo disponível por padrão no Matplotlib

# Plotando o gráfico de barras empilhadas
df.set_index('Ship_State')[['Expedited', 'Standard']].plot(kind='bar', stacked=True, ax=ax, color=['#1f77b4', '#ff7f0e'])

# Adicionando rótulos de texto nas barras
for index, row in df.iterrows():
    total_height = row['Expedited'] + row['Standard']
    # Adiciona texto no meio de cada parte da barra
    ax.text(
        index, 
        row['Expedited'] / 2, 
        f"{row['Expedited']}", 
        ha='center', 
        va='center',
        color='black', 
        fontsize=10,  # Aumentado para melhorar a legibilidade
        fontweight='bold',
        bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', boxstyle='round,pad=0.3')
    )
    ax.text(
        index, 
        row['Expedited'] + row['Standard'] / 2, 
        f"{row['Standard']}", 
        ha='center', 
        va='center',
        color='black', 
        fontsize=10,  # Aumentado para melhorar a legibilidade
        fontweight='bold',
        bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', boxstyle='round,pad=0.3')
    )

# Adicionando título e rótulos dos eixos
ax.set_title('Desempenho de Tipo de Entrega por Estado (Top 10)', fontsize=18)
ax.set_xlabel('Estado', fontsize=14)
ax.set_ylabel('Número de Entregas', fontsize=14)
ax.legend(title='Tipo de Entrega', fontsize=12, labels=['Expedited', 'Standard'])

# Ajuste no layout para evitar sobreposição
plt.xticks(rotation=45, ha='right')
plt.tight_layout()


# Exibindo o gráfico no Streamlit
st.pyplot(fig)