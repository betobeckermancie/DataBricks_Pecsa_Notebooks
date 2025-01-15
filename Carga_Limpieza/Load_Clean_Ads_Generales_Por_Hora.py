# Databricks notebook source
# MAGIC %pip install requests

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.fs.ls("/mnt/processed")


# COMMAND ----------

import pandas as pd

# Ensure the file path includes the correct extension, such as '.csv'
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Hora/anuncios_insights_general_por_hora.csv")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **expandir la columna {actions} crear columnas de cada seccion para su analisis**

# COMMAND ----------

#expandir a detalle columna actions y guardarla
import pandas as pd
import json

#cargar el csv guardado
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Hora/anuncios_insights_general_por_hora.csv")

#Funcion para expandir columna actions
def expand_actions(actions):
    try:
        actions_list = json.loads(actions.replace("'", "\""))
        expanded = {action['action_type']: action['value'] for action in actions_list}
        return pd.Series(expanded)
    except Exception as e:
        print(f"Error al procesar la fila: {e}")
        return pd.Series()
    
#se aplica la funcion a la columna 'actions' y concatena los resultados
df_expanded = df['actions'].apply(expand_actions)

#concatenar las columnas nuevas con el Dataframe original
df_final = pd.concat([df.drop('actions', axis=1), df_expanded], axis=1)

#guardar el resultado en el CSV
df_final.to_csv("/dbfs/mnt/processed/Ads_General_Por_Hora/anuncios_insights_general_por_hora_expanded.csv", index=False)
print(df_final.columns)

#Revisar cambios
display(df_final)
print(df.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Convertir a formato correcto de hora, extraer solo la hora de inicio para tomarlo como referencia**

# COMMAND ----------

import pandas as pd

# Leer el archivo CSV
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Hora/anuncios_insights_general_por_hora_expanded.csv")

# Extraer la hora de inicio de cada intervalo (la primera parte del rango)
df['hora_vista_anuncio'] = df['hourly_stats_aggregated_by_advertiser_time_zone'].str.split(' - ').str[0]

#convertir la hora_inicio al formato string para evitar problemas al guardar
df['hora_vista_anuncio'] =df['hora_vista_anuncio'].astype(str)

# Guardar el dataframe modificado en un nuevo archivo CSV
df.to_csv("/dbfs/mnt/processed/Ads_General_Por_Hora/anuncios_insights_general_por_hora_expanded_data_hour.csv", index=False)

print("CSV modificado y guardado exitosamente.")
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## **Borrar columna hourly_stats_aggregated_by_advertiser_time_zone
# MAGIC ## ya que se extrajo lo necesario**

# COMMAND ----------

import pandas as pd

#leer el archivo desde la ruta especificada
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Hora/anuncios_insights_general_por_hora_expanded_data_hour.csv")

#ELIMINAR LA COLUMNA QUE NO SE OCUPA
df= df.drop(columns=['hourly_stats_aggregated_by_advertiser_time_zone'])

#Guardar el dataframe modificado en un nuevo archivo CSV
df.to_csv("/dbfs/mnt/processed/Ads_General_Por_Hora/anuncios_insights_general_por_hora_expanded_data_hour_clean.csv", index=False)
print("CSV modificado y guardado exitosamente.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cambiar a tipo numerico todos los parametros **necesarios**

# COMMAND ----------

import pandas as pd

df =pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Hora/anuncios_insights_general_por_hora_expanded_data_hour_clean.csv")
#Convertir columnas a tipo numero

columns_to_convert=['onsite_conversion.messaging_user_depth_2_message_send',
       'onsite_conversion.messaging_conversation_started_7d', 'video_view',
       'post_reaction', 'link_click', 'post', 'onsite_conversion.post_save',
       'comment', 'onsite_conversion.messaging_user_depth_3_message_send',
       'like', 'onsite_conversion.messaging_welcome_message_view',
       'onsite_conversion.messaging_conversation_replied_7d',
       'onsite_conversion.messaging_user_depth_5_message_send']
#convertir cada columna a tipo numerico
for column in columns_to_convert:
    if column in df.columns:
        df[column] = pd.to_numeric(df[column], errors='coerce')
    else:
        print(f"Column {column} does not exist in DataFrame.")

#guardar el csv modificado
df.to_csv("/dbfs/mnt/processed/Ads_General_Por_Hora/anuncios_insights_general_por_hora_parametros_numericos.csv", index=False)

#mostrar cambio
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **cambiar columnas del english al spanish**

# COMMAND ----------

#cambiar columnas de csv con mas detalles en spanish
import pandas as pd

#leer el csv desde la ruta especificada
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Hora/anuncios_insights_general_por_hora_parametros_numericos.csv")

#diccionario con los cambios de nombres en columnas/agregar nombres columnas a cambiar por nuevos nombres
new_column_names ={
    'ad_id': 'id_anuncio',
    'ad_name': 'nombre_anuncio',
    'campaign_name': 'nombre_campana',
    'date_start': 'fecha_inicio',
    'date_stop': 'fecha_finalizacion',
    'reach': 'personas_alcanzadas',
    'impressions': 'veces_mostrado',
    'frequency': 'promedio_frecuencia',
    'spend': 'gasto',
    'clicks': 'clicks_en_anuncio',
    'inline_link_clicks': 'click_enlace_trafico',
    #'conversion_rate_ranking': 'conversion_mercado',
    'cpc': 'costo_por_click_anuncio',
    'cpp': 'costo_por_resultado',
    'cpm': 'costo_por_mil_impresiones',
    #'quality_ranking': 'calidad_mercado',
    'buying_type': 'tipo_compra',
    'onsite_conversion.total_messaging_connection': 'conversion_boton_msj',
    'onsite_conversion.messaging_first_reply': 'conversion_primer_respuesta',
    'post_engagement': 'interaccion_post',
    'page_engagement': 'interaccion_page',
    'onsite_conversion.messaging_user_depth_2_message_send': '2do_msj_cliente',
    'onsite_conversion.messaging_conversation_started_7d': 'msj_iniciado_por_cliente_ultimos_7Dias',
    'video_view': 'vistas_video',
    'post_reaction': 'reacciones_post',
    'link_click': 'click_link',
    'post': 'contenido_publicado',
    'onsite_conversion.post_save': 'contenido_guardado',
    'comment': 'comentarios',
    'onsite_conversion.messaging_user_depth_3_message_send': '3er_msj_cliente',
    'like': 'like',
    'onsite_conversion.messaging_welcome_message_view': 'vistas_mensaje_bienvenida',
    'onsite_conversion.messaging_conversation_replied_7d': 'msjs_respondidos_por_pecsa_antes_de_7Dias',
    'onsite_conversion.messaging_user_depth_5_message_send': '5to_msj_cliente'
    # Agrega todos los nombres de columnas que deseas cambiar
    
}

# Renombrar las columnas
df.rename(columns=new_column_names, inplace=True)

# Guardar el DataFrame renombrado en un nuevo archivo CSV
df.to_csv("/dbfs/mnt/processed/Ads_General_Por_Hora/anuncios_insights_general_por_hora_spanish.csv", index=False)

print("Archivo renombrado y guardado exitosamente.")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Cambiar el idioma de las columnas de english a spanish**

# COMMAND ----------

#cambiar el contenido de las columnas de spanish a english
import pandas as pd

#cargar el archivo csv de origen en un dataframe
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Hora/anuncios_insights_general_por_hora_spanish.csv")

#listar las columnas que vamos a remplazar
columns_to_replace=['tipo_compra']

#diccionario con los valores a remplazar y los nuevos valores
replacements = {
    'UNKNOWN': 'desconocido',
    'ABOVE_AVERAGE': 'arriba del promedio',
    'AVERAGE': 'promedio',
    'BELOW_AVERAGE_35': 'abajo del promedio',
    'BELOW_AVERAGE_20': 'abajo del promedio',
    'AUCTION':'subasta'

}

#remplazamos valores 'UNKNOWN' por 'desconocido'
df[columns_to_replace]=df[columns_to_replace].replace(replacements)

#guardar el dataframe en un nuevo archivo csv
df.to_csv("/dbfs/mnt/processed/Ads_General_Por_Hora/anuncios_insights_general_por_hora_limpiado.csv", index=False)
print("Archivo renombrado y guardado exitosamente.")
display(df)

# COMMAND ----------

df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Hora/anuncios_insights_general_por_hora_limpiado.csv")
display(df)
