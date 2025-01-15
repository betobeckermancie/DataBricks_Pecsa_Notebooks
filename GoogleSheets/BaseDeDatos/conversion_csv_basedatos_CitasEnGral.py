# Databricks notebook source

# Crear la base de datos en caso de que no exista
spark.sql("CREATE DATABASE IF NOT EXISTS db_citas")

# Eliminar la tabla si ya existe
spark.sql("DROP TABLE IF EXISTS db_citas.citas_gral")

# Leer el archivo CSV/
df = spark.read.csv("/mnt/GoogleSheets/CSV's_Descargados/Combined_Data.csv", header=True, inferSchema=True)

# Escribir el DataFrame como una nueva tabla
df.write.mode("overwrite").saveAsTable("db_citas.citas_gral")

# Verificar que la tabla se haya creado
spark.sql("SHOW TABLES IN db_citas").show()





# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM db_citas.citas_gral
