# Databricks notebook source
# MAGIC %md
# MAGIC  /dbfs/mnt/PagWeb/Extract/vendidos_web_AllClean.csv

# COMMAND ----------

#crear la base de datos en caso de no existir
spark.sql("CREATE DATABASE IF NOT EXISTS db_pecsa_pagweb")

#eliminar la tabla si ya existe
spark.sql("DROP TABLE IF EXISTS db_pecsa_pagweb.pagweb_vendidos")

#leer el archivo csv
df = spark.read.csv("/mnt/PagWeb/Extract/Vendidos/vendidos_web_AllClean.csv", header=True)

#escribir el dataframe como una nueva table
df.write.mode("overwrite").saveAsTable("db_pecsa_pagweb.pagweb_vendidos")

# verificar que la tabla se haya creado
spark.sql("SHOW TABLES IN db_pecsa_pagweb").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM db_pecsa_pagweb.pagweb_vendidos
