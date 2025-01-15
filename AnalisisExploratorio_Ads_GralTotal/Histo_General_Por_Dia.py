# Databricks notebook source
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Cargar el archivo CSV
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

# Asegurarse de que la columna date_start esté en formato de fecha
df['fecha_inicio'] = pd.to_datetime(df['fecha_inicio'])

# Especificar el parámetro que deseas analizar
parametro_a_analizar = 'vistas_video'  # Cambia este valor por la columna que deseas analizar

# Agrupar por fecha y sumar el parámetro seleccionado
df_grouped = df.groupby('fecha_inicio')[parametro_a_analizar].sum()

# Crear el histograma para mostrar la distribución del parámetro a lo largo del tiempo (por fecha)
plt.figure(figsize=(10, 6))
plt.hist(df_grouped.index, weights=df_grouped.values, bins=30, color='skyblue')

# Formatear las fechas en el eje x para que se muestren con el formato deseado
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d/%m'))
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=3))  # Intervalo de 7 días

# Añadir etiquetas y título dinámico basado en el parámetro
plt.title(f'Histograma de {parametro_a_analizar} a lo largo del tiempo (por Fecha)')
plt.xlabel('Fecha')
plt.ylabel(f'Cantidad de {parametro_a_analizar}')

# Mostrar el gráfico
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Cargar el archivo CSV
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

# Asegurarse de que la columna date_start esté en formato de fecha
df['fecha_inicio'] = pd.to_datetime(df['fecha_inicio'])

# Especificar el parámetro que deseas analizar
parametro_a_analizar = 'veces_mostrado'  # Cambia este valor por la columna que deseas analizar

# Agrupar por fecha y sumar el parámetro seleccionado
df_grouped = df.groupby('fecha_inicio')[parametro_a_analizar].sum()

# Crear el histograma para mostrar la distribución del parámetro a lo largo del tiempo (por fecha)
plt.figure(figsize=(10, 6))
plt.hist(df_grouped.index, weights=df_grouped.values, bins=30, color='skyblue')

# Formatear las fechas en el eje x para que se muestren con el formato deseado
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%y'))
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=7))  # Intervalo de 7 días

# Añadir etiquetas y título dinámico basado en el parámetro
plt.title(f'Histograma de {parametro_a_analizar} a lo largo del tiempo (por Fecha)')
plt.xlabel('Fecha')
plt.ylabel(f'Cantidad de {parametro_a_analizar}')

# Mostrar el gráfico
plt.tight_layout()
plt.show()


# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Cargar el archivo CSV
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

# Asegurarse de que la columna date_start esté en formato de fecha
df['fecha_inicio'] = pd.to_datetime(df['fecha_inicio'])

# Especificar el parámetro que deseas analizar
parametro_a_analizar = 'reacciones_post'  # Cambia este valor por la columna que deseas analizar

# Agrupar por fecha y sumar el parámetro seleccionado
df_grouped = df.groupby('fecha_inicio')[parametro_a_analizar].sum()

# Crear el histograma para mostrar la distribución del parámetro a lo largo del tiempo (por fecha)
plt.figure(figsize=(10, 6))
plt.hist(df_grouped.index, weights=df_grouped.values, bins=30, color='skyblue')

# Formatear las fechas en el eje x para que se muestren con el formato deseado
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%y'))
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=7))  # Intervalo de 7 días

# Añadir etiquetas y título dinámico basado en el parámetro
plt.title(f'Histograma de {parametro_a_analizar} a lo largo del tiempo (por Fecha)')
plt.xlabel('Fecha')
plt.ylabel(f'Cantidad de {parametro_a_analizar}')

# Mostrar el gráfico
plt.tight_layout()
plt.show()


# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Cargar el archivo CSV
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

# Asegurarse de que la columna date_start esté en formato de fecha
df['fecha_inicio'] = pd.to_datetime(df['fecha_inicio'])

# Especificar el parámetro que deseas analizar
parametro_a_analizar = 'promedio_frecuencia'  # Cambia este valor por la columna que deseas analizar

# Agrupar por fecha y sumar el parámetro seleccionado
df_grouped = df.groupby('fecha_inicio')[parametro_a_analizar].sum()

# Crear el histograma para mostrar la distribución del parámetro a lo largo del tiempo (por fecha)
plt.figure(figsize=(10, 6))
plt.hist(df_grouped.index, weights=df_grouped.values, bins=30, color='skyblue')

# Formatear las fechas en el eje x para que se muestren con el formato deseado
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%y'))
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=7))  # Intervalo de 7 días

# Añadir etiquetas y título dinámico basado en el parámetro
plt.title(f'Histograma de {parametro_a_analizar} a lo largo del tiempo (por Fecha)')
plt.xlabel('Fecha')
plt.ylabel(f'Cantidad de {parametro_a_analizar}')

# Mostrar el gráfico
plt.tight_layout()
plt.show()


# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Cargar el archivo CSV
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

# Asegurarse de que la columna date_start esté en formato de fecha
df['fecha_inicio'] = pd.to_datetime(df['fecha_inicio'])

# Especificar el parámetro que deseas analizar
parametro_a_analizar = 'personas_alcanzadas'  # Cambia este valor por la columna que deseas analizar

# Agrupar por fecha y sumar el parámetro seleccionado
df_grouped = df.groupby('fecha_inicio')[parametro_a_analizar].sum()

# Crear el histograma para mostrar la distribución del parámetro a lo largo del tiempo (por fecha)
plt.figure(figsize=(10, 6))
plt.hist(df_grouped.index, weights=df_grouped.values, bins=30, color='skyblue')

# Formatear las fechas en el eje x para que se muestren con el formato deseado
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%y'))
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=7))  # Intervalo de 7 días

# Añadir etiquetas y título dinámico basado en el parámetro
plt.title(f'Histograma de {parametro_a_analizar} a lo largo del tiempo (por Fecha)')
plt.xlabel('Fecha')
plt.ylabel(f'Cantidad de {parametro_a_analizar}')

# Mostrar el gráfico
plt.tight_layout()
plt.show()


# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Cargar el archivo CSV
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

# Asegurarse de que la columna date_start esté en formato de fecha
df['fecha_inicio'] = pd.to_datetime(df['fecha_inicio'])

# Especificar el parámetro que deseas analizar
parametro_a_analizar = 'like'  # Cambia este valor por la columna que deseas analizar

# Agrupar por fecha y sumar el parámetro seleccionado
df_grouped = df.groupby('fecha_inicio')[parametro_a_analizar].sum()

# Crear el histograma para mostrar la distribución del parámetro a lo largo del tiempo (por fecha)
plt.figure(figsize=(10, 6))
plt.hist(df_grouped.index, weights=df_grouped.values, bins=30, color='skyblue')

# Formatear las fechas en el eje x para que se muestren con el formato deseado
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%y'))
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=7))  # Intervalo de 7 días

# Añadir etiquetas y título dinámico basado en el parámetro
plt.title(f'Histograma de {parametro_a_analizar} a lo largo del tiempo (por Fecha)')
plt.xlabel('Fecha')
plt.ylabel(f'Cantidad de {parametro_a_analizar}')

# Mostrar el gráfico
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Cargar el archivo CSV
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

# Asegurarse de que la columna date_start esté en formato de fecha
df['fecha_inicio'] = pd.to_datetime(df['fecha_inicio'])

# Seleccionar las columnas que especificar para sumar
columns_to_sum = ['like', 'reacciones_post', 'comentarios', 'interaccion_post','contenido_guardado','conversion_primer_respuesta','conversion_boton_msj','clicks_en_anuncio','click_link','click_enlace_trafico']

# Agrupar por fecha y sumar las interacciones
df_grouped = df.groupby('fecha_inicio')[columns_to_sum].sum()

# Sumar las columnas seleccionadas para obtener el total de interacciones por fecha
df_grouped['total_interacciones'] = df_grouped.sum(axis=1)

# Crear el histograma para mostrar la distribución de interacciones a lo largo del tiempo (por fecha)
plt.figure(figsize=(10, 6))
plt.hist(df_grouped.index, weights=df_grouped['total_interacciones'], bins=30, color='skyblue')

# Formatear las fechas en el eje x
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%y/%m/%d'))
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=7))

# Añadir etiquetas y título
plt.title('Histograma de Interacciones a lo largo del tiempo (por Fecha)')
plt.xlabel('Fecha')
plt.ylabel('Total de Interacciones')


# Mostrar el gráfico
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Cargar el archivo CSV
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

# Asegurarse de que la columna date_start esté en formato de fecha
df['fecha_inicio'] = pd.to_datetime(df['fecha_inicio'])

# Especificar el parámetro que deseas analizar
parametro_a_analizar = 'interaccion_post'  # Cambia este valor por la columna que deseas analizar

# Agrupar por fecha y sumar el parámetro seleccionado
df_grouped = df.groupby('fecha_inicio')[parametro_a_analizar].sum()

# Crear el histograma para mostrar la distribución del parámetro a lo largo del tiempo (por fecha)
plt.figure(figsize=(10, 6))
plt.hist(df_grouped.index, weights=df_grouped.values, bins=30, color='skyblue')

# Formatear las fechas en el eje x para que se muestren con el formato deseado
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%y'))
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=7))  # Intervalo de 7 días

# Añadir etiquetas y título dinámico basado en el parámetro
plt.title(f'Histograma de {parametro_a_analizar} a lo largo del tiempo (por Fecha)')
plt.xlabel('Fecha')
plt.ylabel(f'Cantidad de {parametro_a_analizar}')

# Mostrar el gráfico
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Cargar el archivo CSV
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

# Asegurarse de que la columna date_start esté en formato de fecha
df['fecha_inicio'] = pd.to_datetime(df['fecha_inicio'])

# Especificar el parámetro que deseas analizar
parametro_a_analizar = 'click_link'  # Cambia este valor por la columna que deseas analizar

# Agrupar por fecha y sumar el parámetro seleccionado
df_grouped = df.groupby('fecha_inicio')[parametro_a_analizar].sum()

# Crear el histograma para mostrar la distribución del parámetro a lo largo del tiempo (por fecha)
plt.figure(figsize=(10, 6))
plt.hist(df_grouped.index, weights=df_grouped.values, bins=30, color='skyblue')

# Formatear las fechas en el eje x para que se muestren con el formato deseado
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%y'))
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=7))  # Intervalo de 7 días

# Añadir etiquetas y título dinámico basado en el parámetro
plt.title(f'Histograma de {parametro_a_analizar} a lo largo del tiempo (por Fecha)')
plt.xlabel('Fecha')
plt.ylabel(f'Cantidad de {parametro_a_analizar}')

# Mostrar el gráfico
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Cargar el archivo CSV
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

# Asegurarse de que la columna date_start esté en formato de fecha
df['fecha_inicio'] = pd.to_datetime(df['fecha_inicio'])

# Especificar el parámetro que deseas analizar
parametro_a_analizar = 'clicks_en_anuncio'  # Cambia este valor por la columna que deseas analizar

# Agrupar por fecha y sumar el parámetro seleccionado
df_grouped = df.groupby('fecha_inicio')[parametro_a_analizar].sum()

# Crear el histograma para mostrar la distribución del parámetro a lo largo del tiempo (por fecha)
plt.figure(figsize=(10, 6))
plt.hist(df_grouped.index, weights=df_grouped.values, bins=30, color='skyblue')

# Formatear las fechas en el eje x para que se muestren con el formato deseado
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%y'))
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=7))  # Intervalo de 7 días

# Añadir etiquetas y título dinámico basado en el parámetro
plt.title(f'Histograma de {parametro_a_analizar} a lo largo del tiempo (por Fecha)')
plt.xlabel('Fecha')
plt.ylabel(f'Cantidad de {parametro_a_analizar}')

# Mostrar el gráfico
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Cargar el archivo CSV
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

# Asegurarse de que la columna date_start esté en formato de fecha
df['fecha_inicio'] = pd.to_datetime(df['fecha_inicio'])

# Especificar el parámetro que deseas analizar
parametro_a_analizar = 'comentarios'  # Cambia este valor por la columna que deseas analizar

# Agrupar por fecha y sumar el parámetro seleccionado
df_grouped = df.groupby('fecha_inicio')[parametro_a_analizar].sum()

# Crear el histograma para mostrar la distribución del parámetro a lo largo del tiempo (por fecha)
plt.figure(figsize=(10, 6))
plt.hist(df_grouped.index, weights=df_grouped.values, bins=30, color='skyblue')

# Formatear las fechas en el eje x para que se muestren con el formato deseado
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%y'))
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=7))  # Intervalo de 7 días

# Añadir etiquetas y título dinámico basado en el parámetro
plt.title(f'Histograma de {parametro_a_analizar} a lo largo del tiempo (por Fecha)')
plt.xlabel('Fecha')
plt.ylabel(f'Cantidad de {parametro_a_analizar}')

# Mostrar el gráfico
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Cargar el archivo CSV
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

# Asegurarse de que la columna date_start esté en formato de fecha
df['fecha_inicio'] = pd.to_datetime(df['fecha_inicio'])

# Especificar el parámetro que deseas analizar
parametro_a_analizar = 'contenido_guardado'  # Cambia este valor por la columna que deseas analizar

# Agrupar por fecha y sumar el parámetro seleccionado
df_grouped = df.groupby('fecha_inicio')[parametro_a_analizar].sum()

# Crear el histograma para mostrar la distribución del parámetro a lo largo del tiempo (por fecha)
plt.figure(figsize=(10, 6))
plt.hist(df_grouped.index, weights=df_grouped.values, bins=30, color='skyblue')

# Formatear las fechas en el eje x para que se muestren con el formato deseado
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d/%m'))
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=2))  # Intervalo de 7 días

# Añadir etiquetas y título dinámico basado en el parámetro
plt.title(f'Histograma de {parametro_a_analizar} a lo largo del tiempo (por Fecha)')
plt.xlabel('Fecha')
plt.ylabel(f'Cantidad de {parametro_a_analizar}')

# Mostrar el gráfico
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Cargar el archivo CSV
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

# Asegurarse de que la columna date_start esté en formato de fecha
df['fecha_inicio'] = pd.to_datetime(df['fecha_inicio'])

# Especificar el parámetro que deseas analizar
parametro_a_analizar = 'conversion_boton_msj'  # Cambia este valor por la columna que deseas analizar

# Agrupar por fecha y sumar el parámetro seleccionado
df_grouped = df.groupby('fecha_inicio')[parametro_a_analizar].sum()

# Crear el histograma para mostrar la distribución del parámetro a lo largo del tiempo (por fecha)
plt.figure(figsize=(10, 6))
plt.hist(df_grouped.index, weights=df_grouped.values, bins=30, color='skyblue')

# Formatear las fechas en el eje x para que se muestren con el formato deseado
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%y'))
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=7))  # Intervalo de 7 días

# Añadir etiquetas y título dinámico basado en el parámetro
plt.title(f'Histograma de {parametro_a_analizar} a lo largo del tiempo (por Fecha)')
plt.xlabel('Fecha')
plt.ylabel(f'Cantidad de {parametro_a_analizar}')

# Mostrar el gráfico
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Cargar el archivo CSV
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

# Asegurarse de que la columna date_start esté en formato de fecha
df['fecha_inicio'] = pd.to_datetime(df['fecha_inicio'])

# Especificar el parámetro que deseas analizar
parametro_a_analizar = 'conversion_primer_respuesta'  # Cambia este valor por la columna que deseas analizar

# Agrupar por fecha y sumar el parámetro seleccionado
df_grouped = df.groupby('fecha_inicio')[parametro_a_analizar].sum()

# Crear el histograma para mostrar la distribución del parámetro a lo largo del tiempo (por fecha)
plt.figure(figsize=(10, 6))
plt.hist(df_grouped.index, weights=df_grouped.values, bins=30, color='skyblue')

# Formatear las fechas en el eje x para que se muestren con el formato deseado
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%y'))
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=7))  # Intervalo de 7 días

# Añadir etiquetas y título dinámico basado en el parámetro
plt.title(f'Histograma de {parametro_a_analizar} a lo largo del tiempo (por Fecha)')
plt.xlabel('Fecha')
plt.ylabel(f'Cantidad de {parametro_a_analizar}')

# Mostrar el gráfico
plt.tight_layout()
plt.show()


# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Cargar el archivo CSV
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

# Asegurarse de que la columna date_start esté en formato de fecha
df['fecha_inicio'] = pd.to_datetime(df['fecha_inicio'])

# Especificar el parámetro que deseas analizar
parametro_a_analizar = 'costo_por_click_anuncio'  # Cambia este valor por la columna que deseas analizar

# Agrupar por fecha y sumar el parámetro seleccionado
df_grouped = df.groupby('fecha_inicio')[parametro_a_analizar].sum()

# Crear el histograma para mostrar la distribución del parámetro a lo largo del tiempo (por fecha)
plt.figure(figsize=(10, 6))
plt.hist(df_grouped.index, weights=df_grouped.values, bins=30, color='skyblue')

# Formatear las fechas en el eje x para que se muestren con el formato deseado
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%y'))
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=7))  # Intervalo de 7 días

# Añadir etiquetas y título dinámico basado en el parámetro
plt.title(f'Histograma de {parametro_a_analizar} a lo largo del tiempo (por Fecha)')
plt.xlabel('Fecha')
plt.ylabel(f'Cantidad de {parametro_a_analizar}')

# Mostrar el gráfico
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Cargar el archivo CSV
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

# Asegurarse de que la columna date_start esté en formato de fecha
df['fecha_inicio'] = pd.to_datetime(df['fecha_inicio'])

# Especificar el parámetro que deseas analizar
parametro_a_analizar = 'costo_por_mil_impresiones'  # Cambia este valor por la columna que deseas analizar

# Agrupar por fecha y sumar el parámetro seleccionado
df_grouped = df.groupby('fecha_inicio')[parametro_a_analizar].sum()

# Crear el histograma para mostrar la distribución del parámetro a lo largo del tiempo (por fecha)
plt.figure(figsize=(10, 6))
plt.hist(df_grouped.index, weights=df_grouped.values, bins=30, color='skyblue')

# Formatear las fechas en el eje x para que se muestren con el formato deseado
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%y'))
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=7))  # Intervalo de 7 días

# Añadir etiquetas y título dinámico basado en el parámetro
plt.title(f'Histograma de {parametro_a_analizar} a lo largo del tiempo (por Fecha)')
plt.xlabel('Fecha')
plt.ylabel(f'Cantidad de {parametro_a_analizar}')

# Mostrar el gráfico
plt.tight_layout()
plt.show()

# COMMAND ----------


import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Cargar el archivo CSV
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

# Asegurarse de que la columna date_start esté en formato de fecha
df['fecha_inicio'] = pd.to_datetime(df['fecha_inicio'])

# Especificar el parámetro que deseas analizar
parametro_a_analizar = 'click_enlace_trafico'  # Cambia este valor por la columna que deseas analizar

# Agrupar por fecha y sumar el parámetro seleccionado
df_grouped = df.groupby('fecha_inicio')[parametro_a_analizar].sum()

# Crear el histograma para mostrar la distribución del parámetro a lo largo del tiempo (por fecha)
plt.figure(figsize=(10, 6))
plt.hist(df_grouped.index, weights=df_grouped.values, bins=30, color='skyblue')

# Formatear las fechas en el eje x para que se muestren con el formato deseado
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%y'))
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=7))  # Intervalo de 7 días

# Añadir etiquetas y título dinámico basado en el parámetro
plt.title(f'Histograma de {parametro_a_analizar} a lo largo del tiempo (por Fecha)')
plt.xlabel('Fecha')
plt.ylabel(f'Cantidad de {parametro_a_analizar}')

# Mostrar el gráfico
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Cargar el archivo CSV
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

# Asegurarse de que la columna date_start esté en formato de fecha
df['fecha_inicio'] = pd.to_datetime(df['fecha_inicio'])

# Especificar el parámetro que deseas analizar
parametro_a_analizar = 'gasto'  # Cambia este valor por la columna que deseas analizar

# Agrupar por fecha y sumar el parámetro seleccionado
df_grouped = df.groupby('fecha_inicio')[parametro_a_analizar].sum()

# Crear el histograma para mostrar la distribución del parámetro a lo largo del tiempo (por fecha)
plt.figure(figsize=(10, 6))
plt.hist(df_grouped.index, weights=df_grouped.values, bins=30, color='skyblue')

# Formatear las fechas en el eje x para que se muestren con el formato deseado
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%y'))
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=7))  # Intervalo de 7 días

# Añadir etiquetas y título dinámico basado en el parámetro
plt.title(f'Histograma de {parametro_a_analizar} a lo largo del tiempo (por Fecha)')
plt.xlabel('Fecha')
plt.ylabel(f'Cantidad de {parametro_a_analizar}')

# Mostrar el gráfico
plt.tight_layout()
plt.show()
