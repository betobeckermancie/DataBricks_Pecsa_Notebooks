# Databricks notebook source
dbutils.fs.rm("/mnt/processed/Ads_General_Por_Hora", True)
dbutils.fs.ls("/mnt/")

# COMMAND ----------

dbutils.fs.ls("/mnt/processed/")
