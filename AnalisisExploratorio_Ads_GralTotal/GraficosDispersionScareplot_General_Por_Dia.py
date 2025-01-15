# Databricks notebook source
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

#cargar el csv en un dataframe
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#filtrar solo las columnas numericas
numeric_columns = df.select_dtypes(include='number').columns

#Iterar sobre todas las columnas numericas y crear scatterplots para compararlas con el gasto
for column in numeric_columns:
    if column != "gasto": #ignorar la columna 'gasto'
        plt.figure(figsize=(8,6))
        sns.scatterplot(x=column, y="gasto", data=df, color="blue")

        #agregar etiquetas y titulos 
        plt.title (f'Scatterplor de {column} vs Gasto')
        plt.xlabel(column)
        plt.ylabel('Gasto($)')

        #mostrar el grafico
        plt.show()
print(df.columns)

