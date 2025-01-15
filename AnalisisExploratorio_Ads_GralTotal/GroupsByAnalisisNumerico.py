# Databricks notebook source
import pandas as pd

# Cargar el archivo CSV
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

# Agrupar por 'nombre_anuncio' y calcular estadísticas descriptivas para las columnas numéricas
df_grouped = df.groupby('nombre_anuncio').mean(numeric_only=True)

# Mostrar el resultado
print(df_grouped)

