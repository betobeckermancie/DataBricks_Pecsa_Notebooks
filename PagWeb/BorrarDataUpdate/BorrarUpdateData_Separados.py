# Databricks notebook source
file_path = "dbfs:/mnt/PagWeb/Extract/Separados"

try:
    dbutils.fs.rm(file_path, True)
    print("Archivo eliminado con éxito.")
except Exception as e:
    print(f"Error al intentar eliminar el archivo: {e}")

# COMMAND ----------

##dbutils.fs.ls("/mnt/PagWeb")
