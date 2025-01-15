# Databricks notebook source
file_path = "dbfs:/mnt/PagWeb/Extract/Vendidos"

try:
    dbutils.fs.ls(file_path, True)
    print("Archivo eliminado con exito")
except Exception as e:
    print("Error al intentar eliminar el archivo: {e}")


# COMMAND ----------

# dbutils.fs.ls("dbfs:/mnt/PagWeb/Extract/Vendidos")
