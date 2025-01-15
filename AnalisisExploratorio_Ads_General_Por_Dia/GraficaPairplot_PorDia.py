# Databricks notebook source
import pandas as pd

df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

display(df)

# COMMAND ----------

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Cargar el archivo CSV
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

# Seleccionar las columnas a analizar
columns_to_analyze = ['nombre_anuncio', 'like', 'reacciones_post', 'veces_mostrado', 'vistas_video']

# Agrupar por nombre_anuncio y obtener los 3 anuncios con mayores 'veces_mostrado'
top_3_anuncios = df.groupby('nombre_anuncio')['veces_mostrado'].sum().nlargest(3).index

# Filtrar el DataFrame para quedarse solo con estos 3 anuncios
df_filtrado = df[df['nombre_anuncio'].isin(top_3_anuncios)]

# Crear el pairplot
plt.figure(figsize=(10, 6))
g=sns.pairplot(df_filtrado[columns_to_analyze],height=1.5,hue='nombre_anuncio', palette='bright')

# Añadir título
plt.suptitle('Relación entre variables para los 3 anuncios con mayores números', y=1.18, fontsize=16)

#mover la leyenda arriba del grafico
g._legend.set_bbox_to_anchor((0.5,1.05))#ajusta posicion
g._legend.set_title("Top 3 Anuncios")

# Ajustar el diseño
plt.tight_layout()
plt.show()

# COMMAND ----------

#Esta combinación te permite ver cómo los clics en los anuncios y las visualizaciones de videos están correlacionados con las conversiones 
#en botones de mensaje y el contenido guardado. Es útil para analizar el comportamiento del usuario a partir de interacciones visuales 
#y su impacto en las conversiones.
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


# Cargar el archivo CSV
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#columnas para anlizar
columns_to_analyze =["nombre_anuncio", "clicks_en_anuncio", "vistas_video", "conversion_boton_msj","contenido_guardado"]

#agrupamos por nombre de anuncio y filtramos los 3 con mayores veces mostrados
top_3_anuncios = df.groupby("nombre_anuncio")["veces_mostrado"].sum().nlargest(3).index

#filtramos df para dejar solo el top 3
df_filtrado = df[df["nombre_anuncio"].isin(top_3_anuncios)]

#creamos el pairplot
plt.figure(figsize=(10,6))
g=sns.pairplot(df_filtrado[columns_to_analyze],height=1.5,hue='nombre_anuncio', palette='bright')

#titulo de cabecera
plt.suptitle("Relación entre variables para los 3 anuncios con mayores números", y=1.18, fontsize=16) 

#mover la leyenda arriba del grafico
g.legend.set_bbox_to_anchor((0.5,1.05)) #ajuste posicion
g.legend.set_title("Top 3 Anuncios")

#mostrar grafico
plt.tight_layout()
plt.show()

# COMMAND ----------

#Esta combinación está enfocada en el rendimiento del anuncio basado 
#en métricas visuales. Permite observar si los anuncios que alcanzan
#más personas y tienen más visualizaciones de video también generan 
#un menor costo por clic y más contenido guardado

import pandas as pd
import seaborn as sns 
import matplotlib.pyplot as plt

#cargar csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#columnas para analizar
columns_to_analyze=["nombre_anuncio", "personas_alcanzadas", "costo_por_click_anuncio","vistas_video","contenido_guardado"]

#agrupamos por nombre de anuncio y filtramos los 3 con mayores veces mostrados
top_3= df.groupby("nombre_anuncio")["veces_mostrado"].sum().nlargest(3).index

#filtramos df para dejar solo el top 3
df_filtrado = df[df["nombre_anuncio"].isin(top_3)]

#creamos el pairplot
plt.figure(figsize=(10, 6))
g=sns.pairplot(df_filtrado[columns_to_analyze],height=1.5, hue='nombre_anuncio', palette='bright')

#titulo el pairplot 
plt.suptitle("Relacion entre variables para los 3 anuncios con mayores numeros", y=1.18, fontsize=16)

#mover la leyenda arriba del grafico
g.legend.set_bbox_to_anchor((0.5,1.05))#ajuste posicion
g.legend.set_title("Top 3 Anuncios")

#mostrar grafico
plt.tight_layout()
plt.show()

# COMMAND ----------

#Esta combinación se centra en la interacción del usuario con el contenido visualizado en los anuncios.
#Like y reacciones_post representan interacciones directas, mientras que veces_mostrado 
#y vistas_video indican la exposición del contenido.

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


# Cargar el archivo CSV
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#columnas para anlizar
columns_to_analyze =["nombre_anuncio", "like", "reacciones_post", "veces_mostrado","vistas_video"]

#agrupamos por nombre de anuncio y filtramos los 3 con mayores veces mostrados
top_3_anuncios = df.groupby("nombre_anuncio")["veces_mostrado"].sum().nlargest(3).index

#filtramos df para dejar solo el top 3
df_filtrado = df[df["nombre_anuncio"].isin(top_3_anuncios)]

#creamos el pairplot
plt.figure(figsize=(10,6))
g=sns.pairplot(df_filtrado[columns_to_analyze],height=1.5,hue='nombre_anuncio', palette='bright')

#titulo de cabecera
plt.suptitle("Relación entre variables para los 3 anuncios con mayores números", y=1.18, fontsize=16) 

#mover la leyenda arriba del grafico
g.legend.set_bbox_to_anchor((0.5,1.05)) #ajuste posicion
g.legend.set_title("Top 3 Anuncios")

#mostrar grafico
plt.tight_layout()
plt.show()

# COMMAND ----------

#Aquí se busca analizar el rendimiento de los anuncios que incluyen un enlace 
#(clicks en enlaces) y cómo están relacionados con las conversiones de primera 
#respuesta, el gasto y el alcance. Es importante para identificar qué anuncios con 
#enlaces obtienen más interacción y a qué costo.


import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

#cargar el csv
df= pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#columnas para analizar
columns_to_analyze = ["nombre_anuncio","click_link","conversion_primer_respuesta","gasto","personas_alcanzadas"]

#agrupamos por nombre de anuncio y filtramos los 3 con mayores veces mostrados
top_3 = df.groupby("nombre_anuncio")["veces_mostrado"].sum().nlargest(3).index

#filtramos df para dejar solo el top 3
df_filtrado =df[df["nombre_anuncio"].isin(top_3)]

#creamos el pairplot
plt.figure(figsize=(10,6))
g=sns.pairplot(df_filtrado[columns_to_analyze],hue='nombre_anuncio',palette='bright')

#titulo de cabecera
plt.suptitle("Relación entre variables para los 3 anuncios con mayores números", y=1.18, fontsize=16)

#mover la leyenda arriba del grafico
g.legend.set_bbox_to_anchor((0.5,1.05))#ajuste posicion
g.legend.set_title("Top 3 anuncios")


#mostrar grafico
plt.tight_layout()
plt.show()

# COMMAND ----------

#Se enfoca en la interacción directa del usuario con el anuncio. Con estas variables,
#puedes evaluar qué tipo de interacción está recibiendo cada anuncio y cómo las 
#reacciones, likes y comentarios están correlacionados entre sí. 
#Es útil para comprender qué anuncios fomentan más interacciones sociales.

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

#cargar el csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#columnas para analizar
columns_to_analyze = ["nombre_anuncio", "interaccion_post", "reacciones_post","comentarios"]

#agrupamos por nombre de anuncio y filtramos los 3 con mayores veces mostrados
top_3_anuncios = df.groupby("nombre_anuncio")["veces_mostrado"].sum().nlargest(3).index

#filtramos df para dejar solo el top 3
df_filtrado = df[df["nombre_anuncio"].isin(top_3_anuncios)]

#creamos el pairplot
plt.figure(figsize=(10, 6))
g=sns.pairplot(df_filtrado[columns_to_analyze], height=1.5,hue='nombre_anuncio',palette="bright")

#titulo de cabecera
plt.suptitle("Relacion entre variables para los 3 anuncios con mayores numeros", y=1.18, fontsize=16)

#mover la leyenda arriba del grafico
g.legend.set_bbox_to_anchor((0.5,1.05))#ajustar posicion
g.legend.set_title("Top 3 anuncios")

#mostrar grafico
plt.tight_layout()
plt.show()

# COMMAND ----------

#El foco de esta combinación está en analizar el tráfico generado por los anuncios 
# y su costo por mil impresiones. Te permitirá observar la relación entre el tráfico 
# generado, la interacción en las publicaciones y la frecuencia con la que se muestran
# los anuncios.

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

#cargar el csv
df =pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#columns para analizar
columns_to_analyze = ["nombre_anuncio","click_enlace_trafico","costo_por_mil_impresiones","interaccion_post","promedio_frecuencia"]

#agrupamos por nombre de anuncio y filtramos los 3 con mayores veces mostrados
top_3 = df.groupby("nombre_anuncio")["veces_mostrado"].sum().nlargest(3).index

#filtramos df para dear solo el top 3
df_filtrado = df[df["nombre_anuncio"].isin(top_3)]

#cremos el pairplot
plt.figure(figsize=(10, 6))
g=sns.pairplot(df_filtrado[columns_to_analyze],hue='nombre_anuncio',palette='bright')

#titulo de cabecera
plt.suptitle("Relación entre variables para los 3 anuncios con mayores números", y=1.18, fontsize=16)

#mover la leyenda arriba del grafico
g.legend.set_bbox_to_anchor((0.5,1.05))#ajuste
g.legend.set_title("Top 3 anuncios")

#mostrar grafico
plt.tight_layout()
plt.show()

# COMMAND ----------

#Esta combinación ayuda a analizar cómo el número de veces que se
#muestra el anuncio (exposición) y el gasto están relacionados con
#los clics y las conversiones en botón de mensaje. 
#Te permite identificar qué anuncios generan más conversiones 
#en relación con la cantidad de veces mostradas y el presupuesto
#gastado.

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

#cargar csv
# Cargar el archivo CSV
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#columnas para analizar
columns_to_analyze=["nombre_anuncio", "clicks_en_anuncio", "conversion_boton_msj","gasto"]

#agrupamos por nombre de anuncio y filtramos los 3 con mayores veces mostrados
top_3_anuncios = df.groupby("nombre_anuncio")['veces_mostrado'].sum().nlargest(3).index

#filtramos df para dejar solo el top 3
df_filtrado =df[df['nombre_anuncio'].isin(top_3_anuncios)]

#creamos el pairplot
plt.figure(figsize=(10, 6))
g=sns.pairplot(df_filtrado[columns_to_analyze],hue='nombre_anuncio', palette='bright')

#titulo de cabecera
plt.suptitle("Relacion entre las variables para los 3 anuncios con mayores numeros", y=1.30,fontsize=16)

#mover la leyenda arriba del grafico
g.legend.set_bbox_to_anchor((0.5,1.4))#ajuste posicion
g.legend.set_title("Top 3 anuncios")

#mostrar grafico
plt.tight_layout()
plt.show()
