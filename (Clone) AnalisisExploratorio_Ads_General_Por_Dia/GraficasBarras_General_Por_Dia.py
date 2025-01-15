# Databricks notebook source
# MAGIC %md
# MAGIC ## # _**Analisis de datos generando una grafica de barras en base a los datos generales sin filtros**_

# COMMAND ----------

# Cargar el CSV en un DataFrame
import pandas as pd
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")
display(df)

# COMMAND ----------

#importamos las libreserias que se necesitan
import pandas as pd
import matplotlib.pyplot as plt

# Cargar el archivo CSV
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

# Agrupar los datos por 'fecha_inicio' para calcular las interacciones por día  agregando todas las metricas para que sean sumadas  en un grupo
df_grouped = df.groupby('fecha_inicio').agg({
    'clicks_en_anuncio': 'sum',
    'veces_mostrado': 'sum',
    'click_enlace_trafico': 'sum',  # Añade otras métricas relevantes que desees
    'interaccion_post': 'sum',
    'conversion_boton_msj': 'sum',
    'contenido_guardado':'sum'
}).reset_index()

# Crear una columna que sume todas las interacciones para cada día
df_grouped['total_interacciones'] = (
    df_grouped['clicks_en_anuncio'] + 
    df_grouped['veces_mostrado'] + 
    df_grouped['click_enlace_trafico'] + 
    df_grouped['interaccion_post']+
    df_grouped['conversion_boton_msj']+
    df_grouped['contenido_guardado']
)

# Ordenar el dataframe por 'total_interacciones' de mayor a menor
df_grouped = df_grouped.sort_values(by='total_interacciones', ascending=False)

# Crear gráfico de barras
plt.figure(figsize=(12, 6))
plt.bar(df_grouped['fecha_inicio'], df_grouped['total_interacciones'], color='blue')

# Etiquetas y título
plt.title('Total de Interacciones por Día', fontsize=16)
plt.xlabel('Fecha', fontsize=14)
plt.ylabel('Total de Interacciones', fontsize=14)
plt.xticks(rotation=90)  # Rotar las etiquetas de los días para mejor legibilidad

# Mostrar gráfico
plt.tight_layout()  # Para ajustar los elementos y que se vean correctamente
plt.show()


# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

# Cargar el archivo CSV
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

# Asegurarse de que la columna date_start esté en formato de fecha
df['fecha_inicio'] = pd.to_datetime(df['fecha_inicio'])

# Extraer el día de la semana (Monday=0, Sunday=6)
df['day_of_week'] = df['fecha_inicio'].dt.dayofweek

# Mapear los valores numéricos a los nombres de los días
dias_semana = {0: 'Lunes', 1: 'Martes', 2: 'Miércoles', 3: 'Jueves', 4: 'Viernes', 5: 'Sábado', 6: 'Domingo'}
df['day_of_week'] = df['day_of_week'].map(dias_semana)

#selecionamos las columnas especificar a sumar
columns_to_sum =['like','reacciones_post','comentarios','interaccion_post','contenido_guardado','conversion_primer_respuesta','conversion_boton_msj','clicks_en_anuncio','click_link','click_enlace_trafico']

# Agrupar por el día de la semana y sumar las interacciones
df_grouped = df.groupby('day_of_week')[columns_to_sum].sum()

#sumar las columnas seleccionadas para obtener el total de interacciones por dia
df_grouped['total_interacciones']=df_grouped.sum(axis=1)

# Ordenar los días de la semana correctamente
df_grouped = df_grouped.reindex(['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo'])

# Crear el gráfico de barras usando solo la columna 'total_interacciones'
plt.figure(figsize=(10, 6))
df_grouped['total_interacciones'].plot(kind='bar', color='skyblue')

# Añadir etiquetas y título
plt.title('Total de todas las interacciones por Día de la Semana')
plt.xlabel('Día de la Semana')
plt.ylabel('Cantidad')

# Mostrar el gráfico
plt.tight_layout()
plt.show()


# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#verificamos que la columna fecha_inicio este en formato fecha
df['fecha_inicio']= pd.to_datetime(df['fecha_inicio'])

#extraemos el dia de la semana 
df['day_of_week'] = df['fecha_inicio'].dt.dayofweek

#Mapear/asignar los valores numericos a los nombres de los dias
dias_semana = {0: 'Lunes', 1:'Martes', 2:"Miercoles", 3:"Jueves",4:"Viernes",5:"Sabado",6:"Domingo"}
df['day_of_week']=df['day_of_week'].map(dias_semana)

#columna dinamica para que cuando cambiemos la variable a analizar se cambie el titulo
columna_a_analizar='like'#aqui cambiare la variable a analizar}

#Agrupare el dia de la semana y sumare las cantiades
df_grouped = df.groupby('day_of_week')[columna_a_analizar].sum()

#Ordeno los dias de la semana en orden correcto
df_grouped = df_grouped.reindex(["Lunes","Martes","Miercoles","Jueves","Viernes","Sabado","Domingo"])

#crear el grafico de barras 
plt.figure(figsize=(10,8))
df_grouped.plot(kind='bar', color='skyblue')

#agregar etiquetas y titulo dinamico
plt.title(f'Total de {columna_a_analizar} por Dia de la Semana')#titulo dinamico
plt.ylabel(f'Cantidad')# etiqueta dinamica para el eje Y

#Mostrar el grafico
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#verificamos que la columna fecha_inicio este en formato fecha
df['fecha_inicio']= pd.to_datetime(df['fecha_inicio'])

#extraemos el dia de la semana 
df['day_of_week'] = df['fecha_inicio'].dt.dayofweek

#Mapear/asignar los valores numericos a los nombres de los dias
dias_semana = {0: 'Lunes', 1:'Martes', 2:"Miercoles", 3:"Jueves",4:"Viernes",5:"Sabado",6:"Domingo"}
df['day_of_week']=df['day_of_week'].map(dias_semana)

#columna dinamica para que cuando cambiemos la variable a analizar se cambie el titulo
columna_a_analizar='reacciones_post'#aqui cambiare la variable a analizar}

#Agrupare el dia de la semana y sumare las cantiades
df_grouped = df.groupby('day_of_week')[columna_a_analizar].sum()

#Ordeno los dias de la semana en orden correcto
df_grouped = df_grouped.reindex(["Lunes","Martes","Miercoles","Jueves","Viernes","Sabado","Domingo"])

#crear el grafico de barras 
plt.figure(figsize=(10,8))
df_grouped.plot(kind='bar', color='skyblue')

#agregar etiquetas y titulo dinamico
plt.title(f'Total de {columna_a_analizar} por Dia de la Semana')#titulo dinamico
plt.ylabel(f'Cantidad')# etiqueta dinamica para el eje Y

#Mostrar el grafico
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#verificamos que la columna fecha_inicio este en formato fecha
df['fecha_inicio']= pd.to_datetime(df['fecha_inicio'])

#extraemos el dia de la semana 
df['day_of_week'] = df['fecha_inicio'].dt.dayofweek

#Mapear/asignar los valores numericos a los nombres de los dias
dias_semana = {0: 'Lunes', 1:'Martes', 2:"Miercoles", 3:"Jueves",4:"Viernes",5:"Sabado",6:"Domingo"}
df['day_of_week']=df['day_of_week'].map(dias_semana)

#columna dinamica para que cuando cambiemos la variable a analizar se cambie el titulo
columna_a_analizar='veces_mostrado'#aqui cambiare la variable a analizar}

#Agrupare el dia de la semana y sumare las cantiades
df_grouped = df.groupby('day_of_week')[columna_a_analizar].sum()

#Ordeno los dias de la semana en orden correcto
df_grouped = df_grouped.reindex(["Lunes","Martes","Miercoles","Jueves","Viernes","Sabado","Domingo"])

#crear el grafico de barras 
plt.figure(figsize=(10,8))
df_grouped.plot(kind='bar', color='skyblue')

#agregar etiquetas y titulo dinamico
plt.title(f'Total de {columna_a_analizar} por Dia de la Semana')#titulo dinamico
plt.ylabel(f'Cantidad')# etiqueta dinamica para el eje Y

#Mostrar el grafico
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#verificamos que la columna fecha_inicio este en formato fecha
df['fecha_inicio']= pd.to_datetime(df['fecha_inicio'])

#extraemos el dia de la semana 
df['day_of_week'] = df['fecha_inicio'].dt.dayofweek

#Mapear/asignar los valores numericos a los nombres de los dias
dias_semana = {0: 'Lunes', 1:'Martes', 2:"Miercoles", 3:"Jueves",4:"Viernes",5:"Sabado",6:"Domingo"}
df['day_of_week']=df['day_of_week'].map(dias_semana)

#columna dinamica para que cuando cambiemos la variable a analizar se cambie el titulo
columna_a_analizar='vistas_video'#aqui cambiare la variable a analizar}

#Agrupare el dia de la semana y sumare las cantiades
df_grouped = df.groupby('day_of_week')[columna_a_analizar].sum()

#Ordeno los dias de la semana en orden correcto
df_grouped = df_grouped.reindex(["Lunes","Martes","Miercoles","Jueves","Viernes","Sabado","Domingo"])

#crear el grafico de barras 
plt.figure(figsize=(10,8))
df_grouped.plot(kind='bar', color='skyblue')

#agregar etiquetas y titulo dinamico
plt.title(f'Total de {columna_a_analizar} por Dia de la Semana')#titulo dinamico
plt.ylabel(f'Cantidad')# etiqueta dinamica para el eje Y

#Mostrar el grafico
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#verificamos que la columna fecha_inicio este en formato fecha
df['fecha_inicio']= pd.to_datetime(df['fecha_inicio'])

#extraemos el dia de la semana 
df['day_of_week'] = df['fecha_inicio'].dt.dayofweek

#Mapear/asignar los valores numericos a los nombres de los dias
dias_semana = {0: 'Lunes', 1:'Martes', 2:"Miercoles", 3:"Jueves",4:"Viernes",5:"Sabado",6:"Domingo"}
df['day_of_week']=df['day_of_week'].map(dias_semana)

#columna dinamica para que cuando cambiemos la variable a analizar se cambie el titulo
columna_a_analizar='promedio_frecuencia'#aqui cambiare la variable a analizar}

#Agrupare el dia de la semana y sumare las cantiades
df_grouped = df.groupby('day_of_week')[columna_a_analizar].sum()

#Ordeno los dias de la semana en orden correcto
df_grouped = df_grouped.reindex(["Lunes","Martes","Miercoles","Jueves","Viernes","Sabado","Domingo"])

#crear el grafico de barras 
plt.figure(figsize=(10,8))
df_grouped.plot(kind='bar', color='skyblue')

#agregar etiquetas y titulo dinamico
plt.title(f'Total de {columna_a_analizar} por Dia de la Semana')#titulo dinamico
plt.ylabel(f'Cantidad')# etiqueta dinamica para el eje Y

#Mostrar el grafico
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#verificamos que la columna fecha_inicio este en formato fecha
df['fecha_inicio']= pd.to_datetime(df['fecha_inicio'])

#extraemos el dia de la semana 
df['day_of_week'] = df['fecha_inicio'].dt.dayofweek

#Mapear/asignar los valores numericos a los nombres de los dias
dias_semana = {0: 'Lunes', 1:'Martes', 2:"Miercoles", 3:"Jueves",4:"Viernes",5:"Sabado",6:"Domingo"}
df['day_of_week']=df['day_of_week'].map(dias_semana)

#columna dinamica para que cuando cambiemos la variable a analizar se cambie el titulo
columna_a_analizar='reacciones_post'#aqui cambiare la variable a analizar}

#Agrupare el dia de la semana y sumare las cantiades
df_grouped = df.groupby('day_of_week')[columna_a_analizar].sum()

#Ordeno los dias de la semana en orden correcto
df_grouped = df_grouped.reindex(["Lunes","Martes","Miercoles","Jueves","Viernes","Sabado","Domingo"])

#crear el grafico de barras 
plt.figure(figsize=(10,8))
df_grouped.plot(kind='bar', color='skyblue')

#agregar etiquetas y titulo dinamico
plt.title(f'Total de {columna_a_analizar} por Dia de la Semana')#titulo dinamico
plt.ylabel(f'Cantidad')# etiqueta dinamica para el eje Y

#Mostrar el grafico
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#verificamos que la columna fecha_inicio este en formato fecha
df['fecha_inicio']= pd.to_datetime(df['fecha_inicio'])

#extraemos el dia de la semana 
df['day_of_week'] = df['fecha_inicio'].dt.dayofweek

#Mapear/asignar los valores numericos a los nombres de los dias
dias_semana = {0: 'Lunes', 1:'Martes', 2:"Miercoles", 3:"Jueves",4:"Viernes",5:"Sabado",6:"Domingo"}
df['day_of_week']=df['day_of_week'].map(dias_semana)

#columna dinamica para que cuando cambiemos la variable a analizar se cambie el titulo
columna_a_analizar='personas_alcanzadas'#aqui cambiare la variable a analizar}

#Agrupare el dia de la semana y sumare las cantiades
df_grouped = df.groupby('day_of_week')[columna_a_analizar].sum()

#Ordeno los dias de la semana en orden correcto
df_grouped = df_grouped.reindex(["Lunes","Martes","Miercoles","Jueves","Viernes","Sabado","Domingo"])

#crear el grafico de barras 
plt.figure(figsize=(10,8))
df_grouped.plot(kind='bar', color='skyblue')

#agregar etiquetas y titulo dinamico
plt.title(f'Total de {columna_a_analizar} por Dia de la Semana')#titulo dinamico
plt.ylabel(f'Cantidad')# etiqueta dinamica para el eje Y

#Mostrar el grafico
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#verificamos que la columna fecha_inicio este en formato fecha
df['fecha_inicio']= pd.to_datetime(df['fecha_inicio'])

#extraemos el dia de la semana 
df['day_of_week'] = df['fecha_inicio'].dt.dayofweek

#Mapear/asignar los valores numericos a los nombres de los dias
dias_semana = {0: 'Lunes', 1:'Martes', 2:"Miercoles", 3:"Jueves",4:"Viernes",5:"Sabado",6:"Domingo"}
df['day_of_week']=df['day_of_week'].map(dias_semana)

#columna dinamica para que cuando cambiemos la variable a analizar se cambie el titulo
columna_a_analizar='comentarios'#aqui cambiare la variable a analizar}

#Agrupare el dia de la semana y sumare las cantiades
df_grouped = df.groupby('day_of_week')[columna_a_analizar].sum()

#Ordeno los dias de la semana en orden correcto
df_grouped = df_grouped.reindex(["Lunes","Martes","Miercoles","Jueves","Viernes","Sabado","Domingo"])

#crear el grafico de barras 
plt.figure(figsize=(10,8))
df_grouped.plot(kind='bar', color='skyblue')

#agregar etiquetas y titulo dinamico
plt.title(f'Total de {columna_a_analizar} por Dia de la Semana')#titulo dinamico
plt.ylabel(f'Cantidad')# etiqueta dinamica para el eje Y

#Mostrar el grafico
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#verificamos que la columna fecha_inicio este en formato fecha
df['fecha_inicio']= pd.to_datetime(df['fecha_inicio'])

#extraemos el dia de la semana 
df['day_of_week'] = df['fecha_inicio'].dt.dayofweek

#Mapear/asignar los valores numericos a los nombres de los dias
dias_semana = {0: 'Lunes', 1:'Martes', 2:"Miercoles", 3:"Jueves",4:"Viernes",5:"Sabado",6:"Domingo"}
df['day_of_week']=df['day_of_week'].map(dias_semana)

#columna dinamica para que cuando cambiemos la variable a analizar se cambie el titulo
columna_a_analizar='interaccion_post'#aqui cambiare la variable a analizar}

#Agrupare el dia de la semana y sumare las cantiades
df_grouped = df.groupby('day_of_week')[columna_a_analizar].sum()

#Ordeno los dias de la semana en orden correcto
df_grouped = df_grouped.reindex(["Lunes","Martes","Miercoles","Jueves","Viernes","Sabado","Domingo"])

#crear el grafico de barras 
plt.figure(figsize=(10,8))
df_grouped.plot(kind='bar', color='skyblue')

#agregar etiquetas y titulo dinamico
plt.title(f'Total de {columna_a_analizar} por Dia de la Semana')#titulo dinamico
plt.ylabel(f'Cantidad')# etiqueta dinamica para el eje Y

#Mostrar el grafico
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#verificamos que la columna fecha_inicio este en formato fecha
df['fecha_inicio']= pd.to_datetime(df['fecha_inicio'])

#extraemos el dia de la semana 
df['day_of_week'] = df['fecha_inicio'].dt.dayofweek

#Mapear/asignar los valores numericos a los nombres de los dias
dias_semana = {0: 'Lunes', 1:'Martes', 2:"Miercoles", 3:"Jueves",4:"Viernes",5:"Sabado",6:"Domingo"}
df['day_of_week']=df['day_of_week'].map(dias_semana)

#columna dinamica para que cuando cambiemos la variable a analizar se cambie el titulo
columna_a_analizar='costo_por_resultado'#aqui cambiare la variable a analizar}

#Agrupare el dia de la semana y sumare las cantiades
df_grouped = df.groupby('day_of_week')[columna_a_analizar].sum()

#Ordeno los dias de la semana en orden correcto
df_grouped = df_grouped.reindex(["Lunes","Martes","Miercoles","Jueves","Viernes","Sabado","Domingo"])

#crear el grafico de barras 
plt.figure(figsize=(10,8))
df_grouped.plot(kind='bar', color='skyblue')

#agregar etiquetas y titulo dinamico
plt.title(f'Total de {columna_a_analizar} por Dia de la Semana')#titulo dinamico
plt.ylabel(f'Cantidad')# etiqueta dinamica para el eje Y

#Mostrar el grafico
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#verificamos que la columna fecha_inicio este en formato fecha
df['fecha_inicio']= pd.to_datetime(df['fecha_inicio'])

#extraemos el dia de la semana 
df['day_of_week'] = df['fecha_inicio'].dt.dayofweek

#Mapear/asignar los valores numericos a los nombres de los dias
dias_semana = {0: 'Lunes', 1:'Martes', 2:"Miercoles", 3:"Jueves",4:"Viernes",5:"Sabado",6:"Domingo"}
df['day_of_week']=df['day_of_week'].map(dias_semana)

#columna dinamica para que cuando cambiemos la variable a analizar se cambie el titulo
columna_a_analizar='costo_por_mil_impresiones'#aqui cambiare la variable a analizar}

#Agrupare el dia de la semana y sumare las cantiades
df_grouped = df.groupby('day_of_week')[columna_a_analizar].sum()

#Ordeno los dias de la semana en orden correcto
df_grouped = df_grouped.reindex(["Lunes","Martes","Miercoles","Jueves","Viernes","Sabado","Domingo"])

#crear el grafico de barras 
plt.figure(figsize=(10,8))
df_grouped.plot(kind='bar', color='skyblue')

#agregar etiquetas y titulo dinamico
plt.title(f'Total de {columna_a_analizar} por Dia de la Semana')#titulo dinamico
plt.ylabel(f'Cantidad')# etiqueta dinamica para el eje Y

#Mostrar el grafico
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#verificamos que la columna fecha_inicio este en formato fecha
df['fecha_inicio']= pd.to_datetime(df['fecha_inicio'])

#extraemos el dia de la semana 
df['day_of_week'] = df['fecha_inicio'].dt.dayofweek

#Mapear/asignar los valores numericos a los nombres de los dias
dias_semana = {0: 'Lunes', 1:'Martes', 2:"Miercoles", 3:"Jueves",4:"Viernes",5:"Sabado",6:"Domingo"}
df['day_of_week']=df['day_of_week'].map(dias_semana)

#columna dinamica para que cuando cambiemos la variable a analizar se cambie el titulo
columna_a_analizar='costo_por_click_anuncio'#aqui cambiare la variable a analizar}

#Agrupare el dia de la semana y sumare las cantiades
df_grouped = df.groupby('day_of_week')[columna_a_analizar].sum()

#Ordeno los dias de la semana en orden correcto
df_grouped = df_grouped.reindex(["Lunes","Martes","Miercoles","Jueves","Viernes","Sabado","Domingo"])

#crear el grafico de barras 
plt.figure(figsize=(10,8))
df_grouped.plot(kind='bar', color='skyblue')

#agregar etiquetas y titulo dinamico
plt.title(f'Total de {columna_a_analizar} por Dia de la Semana')#titulo dinamico
plt.ylabel(f'Cantidad')# etiqueta dinamica para el eje Y

#Mostrar el grafico
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#verificamos que la columna fecha_inicio este en formato fecha
df['fecha_inicio']= pd.to_datetime(df['fecha_inicio'])

#extraemos el dia de la semana 
df['day_of_week'] = df['fecha_inicio'].dt.dayofweek

#Mapear/asignar los valores numericos a los nombres de los dias
dias_semana = {0: 'Lunes', 1:'Martes', 2:"Miercoles", 3:"Jueves",4:"Viernes",5:"Sabado",6:"Domingo"}
df['day_of_week']=df['day_of_week'].map(dias_semana)

#columna dinamica para que cuando cambiemos la variable a analizar se cambie el titulo
columna_a_analizar='conversion_primer_respuesta'#aqui cambiare la variable a analizar}

#Agrupare el dia de la semana y sumare las cantiades
df_grouped = df.groupby('day_of_week')[columna_a_analizar].sum()

#Ordeno los dias de la semana en orden correcto
df_grouped = df_grouped.reindex(["Lunes","Martes","Miercoles","Jueves","Viernes","Sabado","Domingo"])

#crear el grafico de barras 
plt.figure(figsize=(10,8))
df_grouped.plot(kind='bar', color='skyblue')

#agregar etiquetas y titulo dinamico
plt.title(f'Total de {columna_a_analizar} por Dia de la Semana')#titulo dinamico
plt.ylabel(f'Cantidad')# etiqueta dinamica para el eje Y

#Mostrar el grafico
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#verificamos que la columna fecha_inicio este en formato fecha
df['fecha_inicio']= pd.to_datetime(df['fecha_inicio'])

#extraemos el dia de la semana 
df['day_of_week'] = df['fecha_inicio'].dt.dayofweek

#Mapear/asignar los valores numericos a los nombres de los dias
dias_semana = {0: 'Lunes', 1:'Martes', 2:"Miercoles", 3:"Jueves",4:"Viernes",5:"Sabado",6:"Domingo"}
df['day_of_week']=df['day_of_week'].map(dias_semana)

#columna dinamica para que cuando cambiemos la variable a analizar se cambie el titulo
columna_a_analizar='conversion_boton_msj'#aqui cambiare la variable a analizar}

#Agrupare el dia de la semana y sumare las cantiades
df_grouped = df.groupby('day_of_week')[columna_a_analizar].sum()

#Ordeno los dias de la semana en orden correcto
df_grouped = df_grouped.reindex(["Lunes","Martes","Miercoles","Jueves","Viernes","Sabado","Domingo"])

#crear el grafico de barras 
plt.figure(figsize=(10,8))
df_grouped.plot(kind='bar', color='skyblue')

#agregar etiquetas y titulo dinamico
plt.title(f'Total de {columna_a_analizar} por Dia de la Semana')#titulo dinamico
plt.ylabel(f'Cantidad')# etiqueta dinamica para el eje Y

#Mostrar el grafico
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#verificamos que la columna fecha_inicio este en formato fecha
df['fecha_inicio']= pd.to_datetime(df['fecha_inicio'])

#extraemos el dia de la semana 
df['day_of_week'] = df['fecha_inicio'].dt.dayofweek

#Mapear/asignar los valores numericos a los nombres de los dias
dias_semana = {0: 'Lunes', 1:'Martes', 2:"Miercoles", 3:"Jueves",4:"Viernes",5:"Sabado",6:"Domingo"}
df['day_of_week']=df['day_of_week'].map(dias_semana)

#columna dinamica para que cuando cambiemos la variable a analizar se cambie el titulo
columna_a_analizar='contenido_guardado'#aqui cambiare la variable a analizar}

#Agrupare el dia de la semana y sumare las cantiades
df_grouped = df.groupby('day_of_week')[columna_a_analizar].sum()

#Ordeno los dias de la semana en orden correcto
df_grouped = df_grouped.reindex(["Lunes","Martes","Miercoles","Jueves","Viernes","Sabado","Domingo"])

#crear el grafico de barras 
plt.figure(figsize=(10,8))
df_grouped.plot(kind='bar', color='skyblue')

#agregar etiquetas y titulo dinamico
plt.title(f'Total de {columna_a_analizar} por Dia de la Semana')#titulo dinamico
plt.ylabel(f'Cantidad')# etiqueta dinamica para el eje Y

#Mostrar el grafico
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#verificamos que la columna fecha_inicio este en formato fecha
df['fecha_inicio']= pd.to_datetime(df['fecha_inicio'])

#extraemos el dia de la semana 
df['day_of_week'] = df['fecha_inicio'].dt.dayofweek

#Mapear/asignar los valores numericos a los nombres de los dias
dias_semana = {0: 'Lunes', 1:'Martes', 2:"Miercoles", 3:"Jueves",4:"Viernes",5:"Sabado",6:"Domingo"}
df['day_of_week']=df['day_of_week'].map(dias_semana)

#columna dinamica para que cuando cambiemos la variable a analizar se cambie el titulo
columna_a_analizar='clicks_en_anuncio'#aqui cambiare la variable a analizar}

#Agrupare el dia de la semana y sumare las cantiades
df_grouped = df.groupby('day_of_week')[columna_a_analizar].sum()

#Ordeno los dias de la semana en orden correcto
df_grouped = df_grouped.reindex(["Lunes","Martes","Miercoles","Jueves","Viernes","Sabado","Domingo"])

#crear el grafico de barras 
plt.figure(figsize=(10,8))
df_grouped.plot(kind='bar', color='skyblue')

#agregar etiquetas y titulo dinamico
plt.title(f'Total de {columna_a_analizar} por Dia de la Semana')#titulo dinamico
plt.ylabel(f'Cantidad')# etiqueta dinamica para el eje Y

#Mostrar el grafico
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#verificamos que la columna fecha_inicio este en formato fecha
df['fecha_inicio']= pd.to_datetime(df['fecha_inicio'])

#extraemos el dia de la semana 
df['day_of_week'] = df['fecha_inicio'].dt.dayofweek

#Mapear/asignar los valores numericos a los nombres de los dias
dias_semana = {0: 'Lunes', 1:'Martes', 2:"Miercoles", 3:"Jueves",4:"Viernes",5:"Sabado",6:"Domingo"}
df['day_of_week']=df['day_of_week'].map(dias_semana)

#columna dinamica para que cuando cambiemos la variable a analizar se cambie el titulo
columna_a_analizar='click_link'#aqui cambiare la variable a analizar}

#Agrupare el dia de la semana y sumare las cantiades
df_grouped = df.groupby('day_of_week')[columna_a_analizar].sum()

#Ordeno los dias de la semana en orden correcto
df_grouped = df_grouped.reindex(["Lunes","Martes","Miercoles","Jueves","Viernes","Sabado","Domingo"])

#crear el grafico de barras 
plt.figure(figsize=(10,8))
df_grouped.plot(kind='bar', color='skyblue')

#agregar etiquetas y titulo dinamico
plt.title(f'Total de {columna_a_analizar} por Dia de la Semana')#titulo dinamico
plt.ylabel(f'Cantidad')# etiqueta dinamica para el eje Y

#Mostrar el grafico
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#verificamos que la columna fecha_inicio este en formato fecha
df['fecha_inicio']= pd.to_datetime(df['fecha_inicio'])

#extraemos el dia de la semana 
df['day_of_week'] = df['fecha_inicio'].dt.dayofweek

#Mapear/asignar los valores numericos a los nombres de los dias
dias_semana = {0: 'Lunes', 1:'Martes', 2:"Miercoles", 3:"Jueves",4:"Viernes",5:"Sabado",6:"Domingo"}
df['day_of_week']=df['day_of_week'].map(dias_semana)

#columna dinamica para que cuando cambiemos la variable a analizar se cambie el titulo
columna_a_analizar='click_enlace_trafico'#aqui cambiare la variable a analizar}

#Agrupare el dia de la semana y sumare las cantiades
df_grouped = df.groupby('day_of_week')[columna_a_analizar].sum()

#Ordeno los dias de la semana en orden correcto
df_grouped = df_grouped.reindex(["Lunes","Martes","Miercoles","Jueves","Viernes","Sabado","Domingo"])

#crear el grafico de barras 
plt.figure(figsize=(10,8))
df_grouped.plot(kind='bar', color='skyblue')

#agregar etiquetas y titulo dinamico
plt.title(f'Total de {columna_a_analizar} por Dia de la Semana')#titulo dinamico
plt.ylabel(f'Cantidad')# etiqueta dinamica para el eje Y

#Mostrar el grafico
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#verificamos que la columna fecha_inicio este en formato fecha
df['fecha_inicio']= pd.to_datetime(df['fecha_inicio'])

#extraemos el dia de la semana 
df['day_of_week'] = df['fecha_inicio'].dt.dayofweek

#Mapear/asignar los valores numericos a los nombres de los dias
dias_semana = {0: 'Lunes', 1:'Martes', 2:"Miercoles", 3:"Jueves",4:"Viernes",5:"Sabado",6:"Domingo"}
df['day_of_week']=df['day_of_week'].map(dias_semana)

#columna dinamica para que cuando cambiemos la variable a analizar se cambie el titulo
columna_a_analizar='comentarios'#aqui cambiare la variable a analizar}

#Agrupare el dia de la semana y sumare las cantiades
df_grouped = df.groupby('day_of_week')[columna_a_analizar].sum()

#Ordeno los dias de la semana en orden correcto
df_grouped = df_grouped.reindex(["Lunes","Martes","Miercoles","Jueves","Viernes","Sabado","Domingo"])

#crear el grafico de barras 
plt.figure(figsize=(10,8))
df_grouped.plot(kind='bar', color='skyblue')

#agregar etiquetas y titulo dinamico
plt.title(f'Total de {columna_a_analizar} por Dia de la Semana')#titulo dinamico
plt.ylabel(f'Cantidad')# etiqueta dinamica para el eje Y

#Mostrar el grafico
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#verificamos que la columna fecha_inicio este en formato fecha
df['fecha_inicio']= pd.to_datetime(df['fecha_inicio'])

#extraemos el dia de la semana 
df['day_of_week'] = df['fecha_inicio'].dt.dayofweek

#Mapear/asignar los valores numericos a los nombres de los dias
dias_semana = {0: 'Lunes', 1:'Martes', 2:"Miercoles", 3:"Jueves",4:"Viernes",5:"Sabado",6:"Domingo"}
df['day_of_week']=df['day_of_week'].map(dias_semana)

#columna dinamica para que cuando cambiemos la variable a analizar se cambie el titulo
columna_a_analizar='gasto'#aqui cambiare la variable a analizar}

#Agrupare el dia de la semana y sumare las cantiades
df_grouped = df.groupby('day_of_week')[columna_a_analizar].sum()

#Ordeno los dias de la semana en orden correcto
df_grouped = df_grouped.reindex(["Lunes","Martes","Miercoles","Jueves","Viernes","Sabado","Domingo"])

#crear el grafico de barras 
plt.figure(figsize=(10,8))
df_grouped.plot(kind='bar', color='skyblue')

#agregar etiquetas y titulo dinamico
plt.title(f'Total de {columna_a_analizar} por Dia de la Semana')#titulo dinamico
plt.ylabel(f'Cantidad')# etiqueta dinamica para el eje Y

#Mostrar el grafico
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

# Cargar el CSV en un DataFrame
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

# Filtrar solo las columnas numéricas
numeric_columns = df.select_dtypes(include='number').columns

# Iterar sobre todas las columnas numéricas y crear gráficos de barras
for column in numeric_columns:
    # Agrupar por 'nombre_anuncio' y sumar los valores
    grouped_df = df.groupby('nombre_anuncio')[column].sum().sort_values(ascending=False)
    
    # Crear el gráfico de barras
    plt.figure(figsize=(10, 6))
    ax = grouped_df.plot(kind='bar')
    plt.title(f'Gráfico de Barras de {column}', fontsize=16)
    plt.xlabel('Nombre del Anuncio', fontsize=14)
    plt.ylabel(column, fontsize=14)
    plt.grid(True, linestyle='--', alpha=0.7)
    
    # Mostrar el gráfico
    plt.tight_layout()  # Asegurar que todo se vea bien
    plt.show()
