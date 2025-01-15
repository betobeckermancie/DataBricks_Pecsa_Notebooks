# Databricks notebook source
#revisar el contenido de cada columna
import pandas as pd

df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

# Ver las columnas del DataFrame
display(df)

# COMMAND ----------

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Cargar el archivo CSV
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

# Seleccionar solo las columnas numéricas
df_numeric = df.select_dtypes(include='number')

# Excluir una columna específica (por ejemplo, 'clicks_en_anuncio')
df_numeric = df_numeric.drop(columns=['clicks_en_anuncio'])

# Calcular la correlación
correlation_matrix = df_numeric.corr()

# Ajustar el tamaño de la figura para evitar que las etiquetas se amontonen
plt.figure(figsize=(12, 10))

# Crear el heatmap con rotación de las etiquetas para mayor claridad
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt='.2f', 
            linewidths=0.5, annot_kws={"size": 8})  # Puedes ajustar el tamaño del texto de las anotaciones

# Rotar las etiquetas del eje X y Y para que no se superpongan
plt.xticks(rotation=45, ha='right', fontsize=10)
plt.yticks(rotation=0, fontsize=10)

# Añadir el título
plt.title("Heatmap de correlación (sin 'clicks_en_anuncio')", fontsize=14)

# Mostrar el gráfico
plt.tight_layout()
plt.show()


# COMMAND ----------

#eficiencia de los clicks en los ads
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
#correlaciones 
#1 indica una correlacion positiva efectiva perfecta: a medida que una metrica aumenta,
#la otra tambien lo hace
#-1 indica una correlacion negatica perfecta: cuna una metrica aumenta, la otra disminuye
#0 indica que no hay correlacion


# Cargar el archivo CSV
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

# Seleccionar las columnas que te interesen analizar para el heatmap
columns_to_analyze = ['clicks_en_anuncio', 'costo_por_click_anuncio', 'click_enlace_trafico', 'conversion_boton_msj']

# Crear una matriz de correlación entre estas variables
correlation_matrix = df[columns_to_analyze].corr()

# Crear el heatmap con Seaborn
plt.figure(figsize=(10, 6))
sns.heatmap(correlation_matrix, annot=True, cmap="YlGnBu", linewidths=0.5)

# Agregar etiquetas y título
plt.title('Heatmap de eficiencia de los clicks en los ads', fontsize=16)
plt.xticks(rotation=45)
plt.yticks(rotation=0)

# Mostrar el gráfico
plt.tight_layout()
plt.show()


# COMMAND ----------

#Eficiencia del Costo
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
#correlaciones 
#1 indica una correlacion positiva efectiva perfecta: a medida que una metrica aumenta,
#la otra tambien lo hace
#-1 indica una correlacion negatica perfecta: cuna una metrica aumenta, la otra disminuye
#0 indica que no hay correlacion


#carga csv 
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#seleccionar las columnas para analizar
columns_to_analyze = ["costo_por_resultado", "click_enlace_trafico", "costo_por_mil_impresiones","veces_mostrado","costo_por_click_anuncio","conversion_boton_msj"]

#creamos la matriz de correlacion entre las variables
correlation_matrix = df[columns_to_analyze].corr()

# crear el heatmap con seaborn
plt.figure(figsize=(10,6))
sns.heatmap(correlation_matrix, annot=True, cmap="YlGnBu", linewidths=0.5)

#agregar etiquetas y titulo 
plt.title("Heatmap de correlación Eficiencia del Costo", fontsize=16)
plt.xticks(rotation=45)
plt.yticks(rotation=0)

#mostrar el grafico
plt.tight_layout()
plt.show()

# COMMAND ----------

#Relación de Frecuencia y Rendimiento
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

#correlaciones
#1 indica una correlacion positiva efectiva perfecta: a medida que una metrica aumenta,
#la otra tambien lo hace
#-1 indica una correlacion negatica perfecta: cuna una metrica aumenta, la otra disminuye
#0 indica que no hay correlacion

#cargar el csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#seleccionamos las columnas para analizar el heatmap
colums_to_analyze = ["promedio_frecuencia", "personas_alcanzadas", "costo_por_resultado", "veces_mostrado"]

#creamos una matriz de correlacion entre estas variables
correlation_matrix = df[colums_to_analyze].corr()

#creamos el heatmap con seaborn
plt.figure(figsize=(10, 6))
sns.heatmap(correlation_matrix, annot=True, cmap="YlGnBu", linewidths=0.5)

#agregar etiquetas y titulo
plt.title("Heatmap de correlacion entre metricas publicitarias", fontsize="16")
plt.xticks(rotation=45)
plt.yticks(rotation=0)

#mostramos el grafico
plt.tight_layout()
plt.show()

# COMMAND ----------

#relacion entre gasto y metricas de interaccion
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
#correlaciones 
#1 indica una correlacion positiva efectiva perfecta: a medida que una metrica aumenta,
#la otra tambien lo hace
#-1 indica una correlacion negatica perfecta: cuna una metrica aumenta, la otra disminuye
#0 indica que no hay correlacion


#cargar el csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#seleccionar las columnas para analizar por el heatmap
columns_to_analyze= ["gasto", "personas_alcanzadas", "reacciones_post","clicks_en_anuncio"]

# crear una matriz de correlacion entre estas variables
correlation_matrix = df[columns_to_analyze].corr()

#crear heatmap con seaborn
plt.figure(figsize=(10, 6))
sns.heatmap(correlation_matrix, annot=True, cmap="YlGnBu", linewidths=0.5 )

#agregar etiquetas y titulo
plt.title("Heatmap de correlacion entre metricas publicitarias", fontsize="16")
plt.xticks(rotation=45)
plt.yticks(rotation=0)

#Mostrar el grafico
plt.tight_layout()
plt.show

# COMMAND ----------

#Impacto de las Interacciones en la Conversión
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
#correlaciones 
#1 indica una correlacion positiva efectiva perfecta: a medida que una metrica aumenta,
#la otra tambien lo hace
#-1 indica una correlacion negatica perfecta: cuna una metrica aumenta, la otra disminuye
#0 indica que no hay correlacion

#cargar el archivo csv 
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#seleccionar las columnas para analizarlas
columns_to_analyze = ["click_enlace_trafico", "conversion_boton_msj", "reacciones_post","comentarios","contenido_guardado"]

# Verificar si hay valores nulos en la columna 'contenido_guardado' y llenarlos con 0 (podemos cambiar el valor al que queramos)
df['contenido_guardado'].fillna(0, inplace=True)

#creamos mariz de correlacion entre las variables
corelation_matrix = df[columns_to_analyze].corr()

#crear el heatmp con seaborn
plt.figure(figsize=(10,6))
sns.heatmap(corelation_matrix, annot=True, cmap="YlGnBu", linewidths=0.5)

#agregar etiqutadas y titulo
plt.title("Heatmap de Impacto de las Interacciones en la Conversión", fontsize=16)
plt.xticks(rotation=45)
plt.yticks(rotation=0)

#mostrar el grafico
plt.tight_layout()
plt.show()


# COMMAND ----------

#Comparativa de Métricas Visuales
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt 

#correlaciones 
#1 indica una correlacion positiva efectiva perfecta: a medida que una metrica aumenta,
#la otra tambien lo hace
#-1 indica una correlacion negatica perfecta: cuna una metrica aumenta, la otra disminuye
#0 indica que no hay correlacion

#cargar el csv 
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#seleccionar las columnas que se van analizar
columns_to_analyze = ["vistas_video", "contenido_guardado", "click_enlace_trafico"]

# Verificar si hay valores nulos en la columna 'contenido_guardado' y llenarlos con 0 (podemos cambiar el valor al que queramos)
df['contenido_guardado'].fillna(0, inplace=True)

#crear una matriz de correlacion entre variables
correlation_matrix = df[columns_to_analyze].corr()

#creamos heatmap con seaborn
plt.figure(figsize=(10, 6))
sns.heatmap(correlation_matrix, annot=True,cmap="YlGnBu", linewidths=0.5)

#agregar etiquetadas y titulo
plt.title("Heatmap de correlacion entre metricas publicitarias", fontsize=16)
plt.xticks(rotation=45)
plt.yticks(rotation=0)

#mostrar el grafico 
plt.tight_layout()
plt.show()

# COMMAND ----------

#Relación entre el Alcance y las Interacciones
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
#correlaciones 
#1 indica una correlacion positiva efectiva perfecta: a medida que una metrica aumenta,
#la otra tambien lo hace
#-1 indica una correlacion negatica perfecta: cuna una metrica aumenta, la otra disminuye
#0 indica que no hay correlacion

#cargar el csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#seleccionar las columnas para analizar
columns_to_analyze = ["personas_alcanzadas", "veces_mostrado", "vistas_video","interaccion_post"]

#crear una matriz de correlacion entre estas variables
correlation_matrix = df[columns_to_analyze].corr()

#crear el heatmap con Seaborn
plt.figure(figsize=(10, 6))
sns.heatmap(correlation_matrix, annot=True, cmap="YlGnBu", linewidths=0.5)

#agregar etiquetas y titulo
plt.title("Heatmap de correlación entre métricas publicitarias", fontsize=16)
plt.xticks(rotation=45)
plt.yticks(rotation=0)

#Mostrar
plt.tight_layout()
plt.show()
