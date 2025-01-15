# Databricks notebook source
# Definir la ruta de destino para guardar el archivo CSV temporalmente
ruta_temporal = "/mnt/processed/Calendly/temp_citas_julio_nov_2024"

# Importar librerías necesarias
from pyspark.sql import SparkSession


# Leer la tabla desde Unity Catalog
df = spark.sql("SELECT * FROM default.citas_julio_nov_2024")

# Verificar si el DataFrame tiene datos antes de guardarlo
if df.count() > 0:
    # Ruta temporal en DBFS
    ruta_temporal = "/tmp/citas_julio_nov_2024"
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(ruta_temporal)
    
    # Definir la ruta de destino final en el almacenamiento Unity Catalog
    ruta_destino = "/mnt/processed/Calendly/citas_julio_nov_2024.csv"
    
    # Listar archivos en la ruta temporal y mover el archivo CSV a la ruta de destino
    temp_files = dbutils.fs.ls(ruta_temporal)
    for file_info in temp_files:
        if file_info.name.endswith(".csv"):
            # Mover el archivo CSV a la ubicación final deseada
            dbutils.fs.mv(file_info.path, ruta_destino)
            print(f"Archivo CSV guardado en: {ruta_destino}")
            break
else:
    print("El DataFrame está vacío, no se guardará ningún archivo.")





# COMMAND ----------

dbutils.fs.ls("/mnt/processed/")

# COMMAND ----------

import pandas as pd

df = pd.read_csv("/dbfs/mnt/processed/Calendly/citas_julio_nov_2024.csv")
display(df.columns)
