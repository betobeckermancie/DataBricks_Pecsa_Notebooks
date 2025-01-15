# Databricks notebook source
# MAGIC %md
# MAGIC ## **Extraer datos Generales sin filtros**

# COMMAND ----------

import requests
import pandas as pd
import os
import time  # Importamos time para pausar el script con sleep

# Leer el token desde el archivo CSV previamente guardado
df_TokenLargoPlazo = pd.read_csv("/dbfs/mnt/Token_LargoPlazo/long_lived_token.csv")
access_token = df_TokenLargoPlazo["access_token"].iloc[0]

# ID de la cuenta publicitaria de Meta
ad_account_id = "1952584805183463"

# Crear un directorio para guardar los datos si no existe
processed_dir = '/dbfs/mnt/processed/Ads_GralTotal'
if not os.path.exists(processed_dir):
    os.makedirs(processed_dir)

# Función para obtener todas las campañas de una cuenta publicitaria con paginación
def obtener_campanasMeta(access_token, ad_account_id):
    url = f"https://graph.facebook.com/v13.0/act_{ad_account_id}/campaigns"
    params = {
        "access_token": access_token,
        "fields": "id,name,status",
        "limit": 25
    }
    all_campaigns = []
    while url:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            all_campaigns.extend(data['data'])
            url = data.get('paging', {}).get('next')
        else:
            print(f"Error al obtener campañas {response.status_code}: {response.text}")
            break
    return pd.DataFrame(all_campaigns) if all_campaigns else None

# Función para obtener conjuntos de anuncios de una campaña con paginación
def obtener_conjuntos_anunciosMeta(access_token, campaign_id):
    url = f"https://graph.facebook.com/v13.0/{campaign_id}/adsets"
    params = {
        "access_token": access_token,
        "fields": "id,name,status,effective_status",
        "limit": 25
    }
    all_adsets = []
    while url:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            all_adsets.extend(data['data'])
            url = data.get('paging', {}).get('next')
        else:
            print(f"Error al obtener conjuntos de anuncios {response.status_code}: {response.text}")
            break
    return pd.DataFrame(all_adsets) if all_adsets else None

# Función para obtener anuncios de un conjunto de anuncios con paginación
def obtener_anunciosMeta(access_token, adset_id):
    url = f"https://graph.facebook.com/v13.0/{adset_id}/ads"
    params = {
        "access_token": access_token,
        "fields": "id,name,effective_status",
        "limit": 25
    }
    all_ads = []
    while url:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            all_ads.extend(data['data'])
            url = data.get('paging', {}).get('next')
        else:
            print(f"Error al obtener anuncios {response.status_code}: {response.text}")
            break
    return pd.DataFrame(all_ads) if all_ads else None

# Función para obtener métricas (insights) de un anuncio
def obtener_alcance_anuncio(access_token, ad_id):
    url = f"https://graph.facebook.com/v13.0/{ad_id}/insights"
    params = {
        "access_token": access_token,
        "fields": "ad_id,ad_name,adset_name,campaign_name,date_start,date_stop,reach,impressions,frequency,spend,clicks,cost_per_ad_click,inline_link_clicks,conversion_rate_ranking,cpc,cpp,cpm,actions,ad_click_actions,quality_ranking,conversions,buying_type",
        "level": "ad"
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()['data']
        return pd.DataFrame(data) if data else None
    else:
        print(f"Error al obtener insights del anuncio {response.status_code}: {response.text}")
        return None

# Flujo principal
df_campanas = obtener_campanasMeta(access_token, ad_account_id)
if df_campanas is not None:
    all_insights = []  # Lista para acumular los insights de todos los anuncios
    for i, campana in df_campanas.iterrows():
        print(f"Procesando campaña {i + 1} de {len(df_campanas)}: {campana['name']}")
        campaign_id = campana['id']
        df_conjuntos = obtener_conjuntos_anunciosMeta(access_token, campaign_id)
        
        if df_conjuntos is not None:
            for j, conjunto in df_conjuntos.iterrows():
                print(f"  Procesando conjunto de anuncios {j + 1} de {len(df_conjuntos)}: {conjunto['name']}")
                adset_id = conjunto['id']
                df_anuncios = obtener_anunciosMeta(access_token, adset_id)
                
                if df_anuncios is not None:
                    for k, anuncio in df_anuncios.iterrows():
                        print(f"    Procesando anuncio {k + 1} de {len(df_anuncios)}: {anuncio['name']}")
                        ad_id = anuncio['id']
                        df_anuncio_insights = obtener_alcance_anuncio(access_token, ad_id)
                        if df_anuncio_insights is not None:
                            all_insights.append(df_anuncio_insights)

        # Guardar insights al finalizar una campaña
        if all_insights:
            df_all_insights = pd.concat(all_insights, ignore_index=True)
            df_all_insights.to_csv(f"{processed_dir}/ads_GralTotal.csv", index=False)
            print(f"Campaña guardada en el archivo: {campana['name']}")

else:
    print("No se encontraron campañas en la cuenta publicitaria.")

# Verificar archivos creados
dbutils.fs.ls("/mnt/processed/Ads_GralTotal")


# COMMAND ----------

# MAGIC %md
# MAGIC - probando paginacion(limite que se cree 25 campañas) FUNCIONA 

# COMMAND ----------

import requests
import pandas as pd
import os
import time  # Importamos time para pausar el script con sleep

# Leer el token desde el archivo CSV previamente guardado
df_TokenLargoPlazo = pd.read_csv("/dbfs/mnt/Token_LargoPlazo/long_lived_token.csv")
access_token = df_TokenLargoPlazo["access_token"].iloc[0]

# ID de la cuenta publicitaria de Meta
ad_account_id = "1952584805183463"

# Crear un directorio para guardar los datos si no existe
processed_dir = '/dbfs/mnt/processed/Ads_GralTotal'
if not os.path.exists(processed_dir):
    os.makedirs(processed_dir)

# Función para obtener todas las campañas de una cuenta publicitaria con paginación
def obtener_campanasMeta(access_token, ad_account_id):
    """
    Obtiene todas las campañas asociadas a una cuenta publicitaria, usando paginación para manejar resultados grandes.
    """
    url = f"https://graph.facebook.com/v13.0/act_{ad_account_id}/campaigns"
    params = {
        "access_token": access_token,
        "fields": "id,name,status",  # Campos específicos a recuperar
        "limit": 25  # Límite por página
    }
    all_campaigns = []  # Lista para acumular todas las campañas
    while url:  # Mientras exista una URL para la siguiente página
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            all_campaigns.extend(data['data'])
            url = data.get('paging', {}).get('next')  # Obtener la siguiente URL
        else:
            print(f"Error al obtener campañas {response.status_code}: {response.text}")
            break
    return pd.DataFrame(all_campaigns) if all_campaigns else None

# Función para obtener conjuntos de anuncios de una campaña con paginación
def obtener_conjuntos_anunciosMeta(access_token, campaign_id):
    """
    Obtiene todos los conjuntos de anuncios para una campaña específica, usando paginación.
    """
    url = f"https://graph.facebook.com/v13.0/{campaign_id}/adsets"
    params = {
        "access_token": access_token,
        "fields": "id,name,status,effective_status",
        "limit": 25  # Límite por página
    }
    all_adsets = []  # Lista para acumular todos los conjuntos de anuncios
    while url:  # Mientras exista una URL para la siguiente página
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            all_adsets.extend(data['data'])
            url = data.get('paging', {}).get('next')  # Obtener la siguiente URL
        else:
            print(f"Error al obtener conjuntos de anuncios {response.status_code}: {response.text}")
            break
    return pd.DataFrame(all_adsets) if all_adsets else None

# Función para obtener anuncios de un conjunto de anuncios con paginación
def obtener_anunciosMeta(access_token, adset_id):
    """
    Obtiene todos los anuncios asociados a un conjunto de anuncios, usando paginación.
    """
    url = f"https://graph.facebook.com/v13.0/{adset_id}/ads"
    params = {
        "access_token": access_token,
        "fields": "id,name,effective_status",
        "limit": 25  # Límite por página
    }
    all_ads = []  # Lista para acumular todos los anuncios
    while url:  # Mientras exista una URL para la siguiente página
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            all_ads.extend(data['data'])
            url = data.get('paging', {}).get('next')  # Obtener la siguiente URL
        else:
            print(f"Error al obtener anuncios {response.status_code}: {response.text}")
            break
    return pd.DataFrame(all_ads) if all_ads else None

# Función para obtener métricas (insights) de un anuncio
def obtener_alcance_anuncio(access_token, ad_id):
    """
    Recupera las métricas (insights) de un anuncio específico.
    """
    url = f"https://graph.facebook.com/v13.0/{ad_id}/insights"
    params = {
        "access_token": access_token,
        "fields": "ad_id,ad_name,adset_name,campaign_name,date_start,date_stop,reach,impressions,frequency,spend,clicks,cost_per_ad_click,inline_link_clicks,conversion_rate_ranking,cpc,cpp,cpm,actions,ad_click_actions,quality_ranking,conversions,buying_type",  # Métricas solicitadas
        "level": "ad"  # Nivel de la métrica (anuncio)
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()['data']
        return pd.DataFrame(data) if data else None
    else:
        print(f"Error al obtener insights del anuncio {response.status_code}: {response.text}")
        return None

# Flujo principal
df_campanas = obtener_campanasMeta(access_token, ad_account_id)  # Obtener todas las campañas
if df_campanas is not None:
    all_insights = []  # Lista para acumular los insights de todos los anuncios
    for i, campana in df_campanas.iterrows():
        print(f"Procesando campaña {i + 1} de {len(df_campanas)}: {campana['name']}")
        campaign_id = campana['id']
        df_conjuntos = obtener_conjuntos_anunciosMeta(access_token, campaign_id)
        
        # Verificar si excede el límite de conjuntos de anuncios
        if df_conjuntos is not None:
            if len(df_conjuntos) > 13:
                print("Se alcanzaron más de 14 conjuntos de anuncios. Pausando 5 minutos...")
                time.sleep(300)

            for j, conjunto in df_conjuntos.iterrows():
                print(f"  Procesando conjunto de anuncios {j + 1} de {len(df_conjuntos)}: {conjunto['name']}")
                adset_id = conjunto['id']
                df_anuncios = obtener_anunciosMeta(access_token, adset_id)
                
                # Verificar si excede el límite de anuncios
                if df_anuncios is not None:
                    if len(df_anuncios) > 33:
                        print("Se alcanzaron más de 34 anuncios. Pausando 5 minutos...")
                        time.sleep(300)

                    for k, anuncio in df_anuncios.iterrows():
                        print(f"    Procesando anuncio {k + 1} de {len(df_anuncios)}: {anuncio['name']}")
                        ad_id = anuncio['id']
                        df_anuncio_insights = obtener_alcance_anuncio(access_token, ad_id)
                        if df_anuncio_insights is not None:
                            all_insights.append(df_anuncio_insights)

        # Pausa adicional por cada campaña
        print("Esperando 5 minutos antes de procesar la siguiente campaña...")
        time.sleep(300)

    # Guardar todos los insights en un archivo CSV
    if all_insights:
        df_all_insights = pd.concat(all_insights, ignore_index=True)
        df_all_insights.to_csv(f"{processed_dir}/ads_GralTotal.csv", index=False)
        print("Todos los insights de los anuncios guardados exitosamente.")
    else:
        print("No se encontraron insights para los anuncios.")
else:
    print("No se encontraron campañas en la cuenta publicitaria.")

# Verificar archivos creados
dbutils.fs.ls("/mnt/processed/Ads_GralTotal")


# COMMAND ----------

# MAGIC %md
# MAGIC sin paginacion, funcional pero solo obtiene 25 campañas, creo que es el limite de pagina

# COMMAND ----------

import requests
import pandas as pd
import os
import time  # Importamos time para pausar el script con sleep

# Leer el token desde el archivo CSV previamente guardado
df_TokenLargoPlazo = pd.read_csv("/dbfs/mnt/Token_LargoPlazo/long_lived_token.csv")
access_token = df_TokenLargoPlazo["access_token"].iloc[0]

# ID de la cuenta publicitaria de Meta
ad_account_id = "1952584805183463"

# Crear un directorio para guardar los datos si no existe
processed_dir = '/dbfs/mnt/processed/Ads_GralTotal'
if not os.path.exists(processed_dir):
    os.makedirs(processed_dir)

# Función para obtener todas las campañas de una cuenta publicitaria
def obtener_campanasMeta(access_token, ad_account_id):
    """
    Obtiene todas las campañas asociadas a una cuenta publicitaria.
    """
    url = f"https://graph.facebook.com/v13.0/act_{ad_account_id}/campaigns"
    params = {
        "access_token": access_token,
        "fields": "id,name,status"  # Campos específicos a recuperar
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()['data']
        return pd.DataFrame(data) if data else None
    else:
        print(f"Error al obtener campañas {response.status_code}: {response.text}")
        return None

# Función para obtener conjuntos de anuncios de una campaña
def obtener_conjuntos_anunciosMeta(access_token, campaign_id):
    """
    Obtiene todos los conjuntos de anuncios para una campaña específica.
    """
    url = f"https://graph.facebook.com/v13.0/{campaign_id}/adsets"
    params = {
        "access_token": access_token,
        "fields": "id,name,status,effective_status"  # Campos requeridos
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()['data']
        return pd.DataFrame(data) if data else None
    else:
        print(f"Error al obtener conjuntos de anuncios {response.status_code}: {response.text}")
        return None

# Función para obtener anuncios de un conjunto de anuncios
def obtener_anunciosMeta(access_token, adset_id):
    """
    Obtiene todos los anuncios asociados a un conjunto de anuncios.
    """
    url = f"https://graph.facebook.com/v13.0/{adset_id}/ads"
    params = {
        "access_token": access_token,
        "fields": "id,name,effective_status"  # Campos requeridos
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()['data']
        return pd.DataFrame(data) if data else None
    else:
        print(f"Error al obtener anuncios {response.status_code}: {response.text}")
        return None

# Función para obtener métricas (insights) de un anuncio
def obtener_alcance_anuncio(access_token, ad_id):
    """
    Recupera las métricas (insights) de un anuncio específico.
    """
    url = f"https://graph.facebook.com/v13.0/{ad_id}/insights"
    params = {
        "access_token": access_token,
        "fields": "ad_id,ad_name,adset_name,campaign_name,date_start,date_stop,reach,impressions,frequency,spend,clicks,cost_per_ad_click,inline_link_clicks,conversion_rate_ranking,cpc,cpp,cpm,actions,ad_click_actions,quality_ranking,conversions,buying_type",  # Métricas solicitadas
        "level": "ad"  # Nivel de la métrica (anuncio)
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()['data']
        return pd.DataFrame(data) if data else None
    else:
        print(f"Error al obtener insights del anuncio {response.status_code}: {response.text}")
        return None

# Flujo principal
df_campanas = obtener_campanasMeta(access_token, ad_account_id)  # Obtener todas las campañas
if df_campanas is not None:
    all_insights = []  # Lista para acumular los insights de todos los anuncios
    for i, campana in df_campanas.iterrows():
        print(f"Procesando campaña {i + 1} de {len(df_campanas)}: {campana['name']}")
        campaign_id = campana['id']
        df_conjuntos = obtener_conjuntos_anunciosMeta(access_token, campaign_id)
        
        # Verificar si excede el límite de conjuntos de anuncios
        if df_conjuntos is not None:
            if len(df_conjuntos) > 13:
                print("Se alcanzaron más de 14 conjuntos de anuncios. Pausando 5 minutos...")
                time.sleep(300)

            for j, conjunto in df_conjuntos.iterrows():
                print(f"  Procesando conjunto de anuncios {j + 1} de {len(df_conjuntos)}: {conjunto['name']}")
                adset_id = conjunto['id']
                df_anuncios = obtener_anunciosMeta(access_token, adset_id)
                
                # Verificar si excede el límite de anuncios
                if df_anuncios is not None:
                    if len(df_anuncios) > 33:
                        print("Se alcanzaron más de 34 anuncios. Pausando 5 minutos...")
                        time.sleep(300)

                    for k, anuncio in df_anuncios.iterrows():
                        print(f"    Procesando anuncio {k + 1} de {len(df_anuncios)}: {anuncio['name']}")
                        ad_id = anuncio['id']
                        df_anuncio_insights = obtener_alcance_anuncio(access_token, ad_id)
                        if df_anuncio_insights is not None:
                            all_insights.append(df_anuncio_insights)
        # Pausa adicional por cada campaña
        print("Esperando 5 minutos antes de procesar la siguiente campaña...")
        time.sleep(300)

    # Guardar todos los insights en un archivo CSV
    if all_insights:
        df_all_insights = pd.concat(all_insights, ignore_index=True)
        df_all_insights.to_csv(f"{processed_dir}/ads_GralTotalPrueba14112024.csv", index=False)
        print("Todos los insights de los anuncios guardados exitosamente.")
    else:
        print("No se encontraron insights para los anuncios.")
else:
    print("No se encontraron campañas en la cuenta publicitaria.")

# Verificar archivos creados
dbutils.fs.ls("/mnt/processed/Ads_GralTotal")


# COMMAND ----------

# MAGIC %md
# MAGIC prueba funcional (genera 25 campañas , esta por analizar si genera mas y porque)

# COMMAND ----------

import requests
import pandas as pd
import os
import time  # Importar time para usar sleep

# Leer el token desde el archivo CSV previamente guardado
df_TokenLargoPlazo = pd.read_csv("/dbfs/mnt/Token_LargoPlazo/long_lived_token.csv")
access_token = df_TokenLargoPlazo["access_token"].iloc[0]

# Reemplaza este valor con tu ad_account_id (ID de la cuenta publicitaria de Meta)
ad_account_id = "1952584805183463"

# Crear el directorio '/dbfs:/mnt/processed' si no existe
processed_dir = '/dbfs/mnt/processed/Ads_GralTotal'
if not os.path.exists(processed_dir):
    os.makedirs(processed_dir)

# Función para obtener todas las campañas de una cuenta publicitaria
def obtener_campanasMeta(access_token, ad_account_id):
    url = f"https://graph.facebook.com/v13.0/act_{ad_account_id}/campaigns"
    params = {
        "access_token": access_token,
        "fields": "id,name,status"  # Solo los campos necesarios
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()['data']
        return pd.DataFrame(data) if data else None
    else:
        print(f"Error al obtener campañas {response.status_code}: {response.text}")
        return None

# Función para obtener todos los conjuntos de anuncios de todas las campañas
def obtener_conjuntos_anunciosMeta(access_token, campaign_id):
    url = f"https://graph.facebook.com/v13.0/{campaign_id}/adsets"
    params = {
        "access_token": access_token,
        "fields": "id,name,status,effective_status"
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()['data']
        return pd.DataFrame(data) if data else None
    else:
        print(f"Error al obtener conjuntos de anuncios {response.status_code}: {response.text}")
        return None

# Función para obtener los anuncios de un conjunto de anuncios
def obtener_anunciosMeta(access_token, adset_id):
    url = f"https://graph.facebook.com/v13.0/{adset_id}/ads"
    params = {
        "access_token": access_token,
        "fields": "id,name,effective_status"
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()['data']
        return pd.DataFrame(data) if data else None
    else:
        print(f"Error al obtener anuncios {response.status_code}: {response.text}")
        return None

# Función para obtener el alcance de un anuncio mediante el endpoint de insights
def obtener_alcance_anuncio(access_token, ad_id):
    url = f"https://graph.facebook.com/v13.0/{ad_id}/insights"
    params = {
        "access_token": access_token,
        "fields": "ad_id,ad_name,adset_name,campaign_name,date_start,date_stop,reach,impressions,frequency,spend,clicks,cost_per_ad_click,inline_link_clicks,conversion_rate_ranking,cpc,cpp,cpm,actions,ad_click_actions,quality_ranking,conversions,buying_type",
        "level": "ad"
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()['data']
        return pd.DataFrame(data) if data else None
    else:
        print(f"Error al obtener insights del anuncio {response.status_code}: {response.text}")
        return None

# Flujo principal
df_campanas = obtener_campanasMeta(access_token, ad_account_id)
if df_campanas is not None:
    all_insights = []
    for i, campana in df_campanas.iterrows():
        print(f"Procesando campaña {i + 1} de {len(df_campanas)}")
        campaign_id = campana['id']
        df_conjuntos = obtener_conjuntos_anunciosMeta(access_token, campaign_id)
        if df_conjuntos is not None:
            for j, conjunto in df_conjuntos.iterrows():
                print(f"  Procesando conjunto de anuncios {j + 1} de {len(df_conjuntos)}")
                adset_id = conjunto['id']
                df_anuncios = obtener_anunciosMeta(access_token, adset_id)
                if df_anuncios is not None:
                    for k, anuncio in df_anuncios.iterrows():
                        print(f"    Procesando anuncio {k + 1} de {len(df_anuncios)}")
                        ad_id = anuncio['id']
                        df_anuncio_insights = obtener_alcance_anuncio(access_token, ad_id)
                        if df_anuncio_insights is not None:
                            all_insights.append(df_anuncio_insights)
        # Pausa de 5 minutos entre campañas
        print("Esperando 5 minutos antes de continuar con la siguiente campaña...")
        time.sleep(300)

    # Guardar todos los insights en un solo archivo CSV
    if all_insights:
        df_all_insights = pd.concat(all_insights, ignore_index=True)
        df_all_insights.to_csv(f"{processed_dir}/ads_GralTotal.csv", index=False)
        print("Todos los insights de los anuncios guardados exitosamente.")
    else:
        print("No se encontraron insights para los anuncios.")
else:
    print("No se encontraron campañas en la cuenta publicitaria.")

# Verificar archivos creados
dbutils.fs.ls("/mnt/processed/Ads_GralTotal")





# COMMAND ----------

#codigo para guardar todos los insights necesarios para el analisis de datos de SOLO los anuncios
import requests
import pandas as pd
import os

# Leer el token desde el archivo CSV previamente guardado
df_TokenLargoPlazo = pd.read_csv("/dbfs/mnt/Token_LargoPlazo/long_lived_token.csv")
access_token = df_TokenLargoPlazo["access_token"].iloc[0]

# Reemplaza este valor con tu ad_account_id (ID de la cuenta publicitaria de Meta)
ad_account_id = "1952584805183463"

# Crear el directorio '/dbfs:/mnt/processed' si no existe
processed_dir = '/dbfs/mnt/processed/Ads_GralTotal'
if not os.path.exists(processed_dir):
    os.makedirs(processed_dir)

# Función para obtener todos los conjuntos de anuncios de todas las campañas
def obtener_conjuntos_anunciosMeta(access_token, ad_account_id):
    url = f"https://graph.facebook.com/v13.0/act_{ad_account_id}/adsets"
    params = {
        "access_token": access_token,
        "fields": "id,name,status"  # Especifica las columnas que deseas obtener
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()['data']
        df_conjuntosMeta = pd.DataFrame(data)
        return df_conjuntosMeta
    else:
        print(f"Error {response.status_code}: {response.text}")
        return None

# Función para obtener el alcance de un anuncio mediante el endpoint de insights
def obtener_alcance_anuncio(access_token, ad_id):
    url = f"https://graph.facebook.com/v13.0/{ad_id}/insights"
    params = {
        "access_token": access_token,
        "fields": "ad_id,ad_name,adset_name,campaign_name,date_start,date_stop,reach,impressions,frequency,spend,clicks,cost_per_ad_click,inline_link_clicks,conversion_rate_ranking,cpc,cpp,cpm,actions,ad_click_actions,quality_ranking,conversions,buying_type",  # Especifica las métricas que deseas obtener
        "level": "ad",  # Nivel de la métrica, en este caso es a nivel de anuncio
        
    }

    
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()['data']
        df_insights = pd.DataFrame(data)
        return df_insights
    else:
        print(f"Error {response.status_code}: {response.text}")
        return None

# Obtener todos los conjuntos de anuncios
df_conjuntos = obtener_conjuntos_anunciosMeta(access_token, ad_account_id)

if df_conjuntos is not None:
    all_insights = []
    # Obtener y guardar el alcance y otras métricas para cada anuncio en los conjuntos de anuncios
    for adset_id in df_conjuntos['id']:
        url = f"https://graph.facebook.com/v13.0/{adset_id}/ads"
        params = {
            "access_token": access_token,
            "fields": "id,name"  # Obtener solo los IDs de los anuncios
        }
        response = requests.get(url, params=params)
        if response.status_code == 200:
            ads_data = response.json()['data']
            for ad in ads_data:
                ad_id = ad['id']
                df_anuncio_insights = obtener_alcance_anuncio(access_token, ad_id)
                if df_anuncio_insights is not None:
                    all_insights.append(df_anuncio_insights)

    # Unir todos los DataFrames de insights en uno solo
    if all_insights:
        df_all_insights = pd.concat(all_insights, ignore_index=True)
        # Guardar todos los insights en un solo CSV
        df_all_insights.to_csv(f"{processed_dir}/ads_GralTotal.csv", index=False)
        print("Todos los insights de los anuncios generales por hora guardados exitosamente.")
    else:
        print("No se encontraron insights de anuncios.")
else:
    print("No se encontraron conjuntos de anuncios o limite de solicitud excedido.")
#revisar cuales archivos existen dentro de la ruta
dbutils.fs.ls("/mnt/processed/Ads_GralTotal")

# COMMAND ----------

#revisar cuales archivos existen dentro de la ruta
dbutils.fs.ls("/mnt/processed/Ads_GralTotal")

# COMMAND ----------

import pandas as pd

# Cargar el archivo generado
file_path = '/dbfs/mnt/processed/Ads_GralTotal/ads_GralTotal.csv'
df = pd.read_csv(file_path)

# Ver las dimensiones del DataFrame
print(f"Total filas: {df.shape[0]}, Total columnas: {df.shape[1]}")

# Mostrar una muestra de los datos
display(df.head(200))  # Cambia el número para mostrar más filas

# COMMAND ----------

#revisar el contenido de cada columna
import pandas as pd
#crear dataframe para leer el csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_GralTotal/ads_GralTotal.csv")

# Ver las columnas del DataFrame
display(df.columns)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC probando

# COMMAND ----------

#borrar archivos expecificos dentro de una carpeta
dbutils.fs.rm("/mnt/processed/Ads_GralTotal", True)

