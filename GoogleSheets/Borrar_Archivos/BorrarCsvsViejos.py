# Databricks notebook source
import os
file_path = "dbfs:/mnt/GoogleSheets/CSV's_Descargados/"

try:
    dbutils.fs.rm(file_path, True)
    print("Archivo eliminado con éxito.")
except Exception as e:
    print(f"Error al intentar eliminar el archivo: {e}")

dbutils.fs.ls("mnt/GoogleSheets/")
