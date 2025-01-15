# Databricks notebook source
# MAGIC %md
# MAGIC /dbfs/mnt/PagWeb/Extract/separados_web_AllCleaned.csv

# COMMAND ----------

# Crear la base de datos en caso de que no exista
spark.sql("CREATE DATABASE IF NOT EXISTS db_pecsa_pagweb")

# Eliminar la tabla si ya existe
spark.sql("DROP TABLE IF EXISTS db_pecsa_pagweb.pagweb_separados")

# Leer el archivo CSV
df = spark.read.csv("/mnt/PagWeb/Extract/Separados/separados_web_AllClean.csv", header=True, inferSchema=True)

# Escribir el DataFrame como una nueva tabla
df.write.mode("overwrite").saveAsTable("db_pecsa_pagweb.pagweb_separados")

# Verificar que la tabla se haya creado
spark.sql("SHOW TABLES IN db_pecsa_pagweb").show()




# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM db_pecsa_pagweb.pagweb_separados
