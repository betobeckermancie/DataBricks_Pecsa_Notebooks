# Databricks notebook source
# MAGIC %pip install requests

# COMMAND ----------

#reiniciar python para usar lib instaladas
dbutils.library.restartPython()


# COMMAND ----------

#borrar todos los archivos de la carpeta
dbutils.fs.rm("/mnt/processed/Token_LargoPlazo/", recurse=True)

#recrear carpeta
#dbutils.fs.mkdirs("/mnt/processed")


# COMMAND ----------

#revisar cambios
dbutils.fs.ls("/mnt/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convertimos el loken de corta duracion(60hrs) a (60 dias)

# COMMAND ----------

import requests
import pandas as pd

# Variables 
app_id = "3582922195333256"  # ID aplicación de Facebook
app_secret = "8c384c7385b10600cc04ce6112843939"  #Clave Secreta de aplicación
short_lived_token = "EAAy6phS18IgBOzVlEbe7NmwA8MJJIZCYmMIWV50GBLwBftXrUqdLCYexacragipz6W1Dp9Q4MgcZCfof8YHAWLYjUt6ZB7ogeWZArYlWA6swMmZCQK57a1rNj3SNLqkcdRRmssPa0dqu3fo9auwmxAg0p0lQNt6XtCFC19mcBmKILGi3FWaJjAZBYLkSEZBNUJMCqSZBk8sZCYV26rqlDZCcJOq6Szp1UZD"  # Token de acceso de corto plazo

# URL para intercambiar el token de corto plazo por uno de largo plazo
url = f"https://graph.facebook.com/v13.0/oauth/access_token?grant_type=fb_exchange_token&client_id={app_id}&client_secret={app_secret}&fb_exchange_token={short_lived_token}"

# Realizar la solicitud
response = requests.get(url)

# Verificar el resultado
if response.status_code == 200:
    # Extraer el token de largo plazo del resultado de la API
    long_lived_token = response.json().get("access_token")
    print("Nuevo Token de Largo Plazo:", long_lived_token)
    
    # Guarda el token de largo plazo en un archivo CSV
    df_Token_LargoPlazo = pd.DataFrame({"access_token": [long_lived_token]})#extraemos el token de largo plazo
    df_Token_LargoPlazo.to_csv("/dbfs/mnt/Token_LargoPlazo/long_lived_token.csv", index=False)
    print("Token de Largo Plazo guardado exitosamente.")
else:
    # Mostrar el error si ocurre
    print("Error al intentar renovar el token:", response.text)

# COMMAND ----------

#revisar cambios 
dbutils.fs.ls("/mnt/Token_LargoPlazo/")
