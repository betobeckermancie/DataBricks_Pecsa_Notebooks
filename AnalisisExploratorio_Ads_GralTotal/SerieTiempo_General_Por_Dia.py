# Databricks notebook source
import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df= pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#confirmar que la columna fecha_inicio sea tipo datetime
df['fecha_inicio'] = pd.to_datetime(df["fecha_inicio"])

#establecer la columna 'fecha_inicio' como indice para aplicar a la time serie
df.set_index("fecha_inicio", inplace=True)

#se usa para hacer dinamico el titulo
variable_analizar ="click_enlace_trafico" #cambiar cuanndo sea necesario

#seleccionar las columnas para analizar
df_time_series = df[variable_analizar]

#resamplear los datos segun sea necesario analizar Dia 'D',
#semana 'W' o año 'Y'
df_resampled = df_time_series.resample("D").sum()

#se usa para hacer dinamicas las etiquetas
if df_time_series.resample("D"):
    fecha="Dia"
elif df_time_series.resample("W"):
    fecha="Semana"
elif df_time_series.resample("M"):
    fecha="Mes"
else:
    fecha="Anual"

#Graficar la serie de tiempo
plt.figure(figsize=(10, 6))
df_resampled.plot(title=f"Serie de tiempo de {variable_analizar} por dia", color="blue")

#agregar etiquetas a la grafica
plt.xlabel(f"Analisis de {fecha}")
plt.ylabel("Total")
plt.grid(True)

#mostrar la grafica
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df= pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#confirmar que la columna fecha_inicio sea tipo datetime
df['fecha_inicio'] = pd.to_datetime(df["fecha_inicio"])

#establecer la columna 'fecha_inicio' como indice para aplicar a la time serie
df.set_index("fecha_inicio", inplace=True)

#se usa para hacer dinamico el titulo
variable_analizar ="click_link" #cambiar cuanndo sea necesario

#seleccionar las columnas para analizar
df_time_series = df[variable_analizar]

#resamplear los datos segun sea necesario analizar Dia 'D',
#semana 'W' o año 'Y'
df_resampled = df_time_series.resample("D").sum()

#se usa para hacer dinamicas las etiquetas
if df_time_series.resample("D"):
    fecha="Dia"
elif df_time_series.resample("W"):
    fecha="Semana"
elif df_time_series.resample("M"):
    fecha="Mes"
else:
    fecha="Anual"

#Graficar la serie de tiempo
plt.figure(figsize=(10, 6))
df_resampled.plot(title=f"Serie de tiempo de {variable_analizar} por dia", color="blue")

#agregar etiquetas a la grafica
plt.xlabel(f"Analisis de {fecha}")
plt.ylabel("Total")
plt.grid(True)

#mostrar la grafica
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df= pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#confirmar que la columna fecha_inicio sea tipo datetime
df['fecha_inicio'] = pd.to_datetime(df["fecha_inicio"])

#establecer la columna 'fecha_inicio' como indice para aplicar a la time serie
df.set_index("fecha_inicio", inplace=True)

#se usa para hacer dinamico el titulo
variable_analizar ="clicks_en_anuncio" #cambiar cuanndo sea necesario

#seleccionar las columnas para analizar
df_time_series = df[variable_analizar]

#resamplear los datos segun sea necesario analizar Dia 'D',
#semana 'W' o año 'Y'
df_resampled = df_time_series.resample("D").sum()

#se usa para hacer dinamicas las etiquetas
if df_time_series.resample("D"):
    fecha="Dia"
elif df_time_series.resample("W"):
    fecha="Semana"
elif df_time_series.resample("M"):
    fecha="Mes"
else:
    fecha="Anual"

#Graficar la serie de tiempo
plt.figure(figsize=(10, 6))
df_resampled.plot(title=f"Serie de tiempo de {variable_analizar} por dia", color="blue")

#agregar etiquetas a la grafica
plt.xlabel(f"Analisis de {fecha}")
plt.ylabel("Total")
plt.grid(True)

#mostrar la grafica
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#establecer la columna 'fecha_inicio' como indice para aplicar l la time serie
df['fecha_inicio'] = pd.to_datetime(df["fecha_inicio"])

#establecer la columna 'fecha_inicio' como indice para aplicar a la time serie
df.set_index("fecha_inicio", inplace=True)

#se usa para hacer dinamico el titulo
variable_analizar = "comentarios" #cambiar cuando sea necesario

#seleccionamos las columnas para analizar
df_time_series = df[variable_analizar]

#resamplear los datos segun sea necesario analizar Dia 'D',
#semana 'W' o año 'Y'
df_resampled = df_time_series.resample("D").sum()

#se usa para hacer dinamicas las etiquetas
if df_time_series.resample("D"):
    fecha="Dia"
elif df_time_series.resample("W"):
    fecha="Semana"
elif df_time_series.resample("M"):
    fecha="Mes"
else:
    fecha="Anual"

#Graficar la serie de tiempo
plt.figure(figsize=(10, 8))
df_resampled.plot(title=f"Serie de tiempo de {variable_analizar} por dia", color="blue")

#agregar etiquetas para la grafica
plt.xlabel(f"Analisi de {fecha}")
plt.ylabel("Total")
plt.grid(True)

#mostrar la grafica 
plt.tight_layout()
plt.show()


# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df= pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#confirmar que la columna fecha_inicio sea tipo datetime
df['fecha_inicio'] = pd.to_datetime(df["fecha_inicio"])

#establecer la columna 'fecha_inicio' como indice para aplicar a la time serie
df.set_index("fecha_inicio", inplace=True)

#se usa para hacer dinamico el titulo
variable_analizar ="contenido_guardado" #cambiar cuanndo sea necesario

#seleccionar las columnas para analizar
df_time_series = df[variable_analizar]

#resamplear los datos segun sea necesario analizar Dia 'D',
#semana 'W' o año 'Y'
df_resampled = df_time_series.resample("D").sum()

#se usa para hacer dinamicas las etiquetas
if df_time_series.resample("D"):
    fecha="Dia"
elif df_time_series.resample("W"):
    fecha="Semana"
elif df_time_series.resample("M"):
    fecha="Mes"
else:
    fecha="Anual"

#Graficar la serie de tiempo
plt.figure(figsize=(10, 6))
df_resampled.plot(title=f"Serie de tiempo de {variable_analizar} por dia", color="blue")

#agregar etiquetas a la grafica
plt.xlabel(f"Analisis de {fecha}")
plt.ylabel("Total")
plt.grid(True)

#mostrar la grafica
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df= pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#confirmar que la columna fecha_inicio sea tipo datetime
df['fecha_inicio'] = pd.to_datetime(df["fecha_inicio"])

#establecer la columna 'fecha_inicio' como indice para aplicar a la time serie
df.set_index("fecha_inicio", inplace=True)

#se usa para hacer dinamico el titulo
variable_analizar ="conversion_primer_respuesta" #cambiar cuanndo sea necesario

#seleccionar las columnas para analizar
df_time_series = df[variable_analizar]

#resamplear los datos segun sea necesario analizar Dia 'D',
#semana 'W' o año 'Y'
df_resampled = df_time_series.resample("D").sum()

#se usa para hacer dinamicas las etiquetas
if df_time_series.resample("D"):
    fecha="Dia"
elif df_time_series.resample("W"):
    fecha="Semana"
elif df_time_series.resample("M"):
    fecha="Mes"
else:
    fecha="Anual"

#Graficar la serie de tiempo
plt.figure(figsize=(10, 6))
df_resampled.plot(title=f"Serie de tiempo de {variable_analizar} por dia", color="blue")

#agregar etiquetas a la grafica
plt.xlabel(f"Analisis de {fecha}")
plt.ylabel("Total")
plt.grid(True)

#mostrar la grafica
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#confirmar que la columna fecha_inicio sea tipo datatime
df['fecha_inicio'] = pd.to_datetime(df['fecha_inicio'])

#establezco la columna 'fecha_inicio' como el indice del dataframe para aplicar la time_serie
df.set_index("fecha_inicio", inplace=True)

#establezco la variable a analizar para usarlo de forma dinamica
variable_analizar = "conversion_boton_msj"#cambiar cuando sea necesario

#seleccionar las columnas para analizar
df_time_series =df[variable_analizar]

#remplazar los datos segun sea necesario analizar ya sea
# dia 'D', semana 'W', mes 'M' o año 'Y'.
df_resampled = df_time_series.resample('D').sum()

#se usa para hacer dinamicas las etiquetas
if df_time_series.resample('D'):
    fecha = "Dia"
elif df_time_series.resample('W'):
    fecha = "Semana"
elif df_time_series.resample('M'):
    fecha = "Mes"
else:
    fecha = "Anual"

    
# Graficar la serie de tiempo
plt.figure(figsize=(10,6))
df_resampled.plot(title=f"Serie de tiempo de {variable_analizar} por dia", color='blue')

#agregar etiquetas a grafica
plt.xlabel(f"Analis  {fecha}")
plt.ylabel("Total")
plt.grid(True)

#mostrar la grafica
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df= pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#confirmar que la columna fecha_inicio sea tipo datetime
df['fecha_inicio'] = pd.to_datetime(df["fecha_inicio"])

#establecer la columna 'fecha_inicio' como indice para aplicar a la time serie
df.set_index("fecha_inicio", inplace=True)

#se usa para hacer dinamico el titulo
variable_analizar ="costo_por_click_anuncio" #cambiar cuanndo sea necesario

#seleccionar las columnas para analizar
df_time_series = df[variable_analizar]

#resamplear los datos segun sea necesario analizar Dia 'D',
#semana 'W' o año 'Y'
df_resampled = df_time_series.resample("D").sum()

#se usa para hacer dinamicas las etiquetas
if df_time_series.resample("D"):
    fecha="Dia"
elif df_time_series.resample("W"):
    fecha="Semana"
elif df_time_series.resample("M"):
    fecha="Mes"
else:
    fecha="Anual"

#Graficar la serie de tiempo
plt.figure(figsize=(10, 6))
df_resampled.plot(title=f"Serie de tiempo de {variable_analizar} por dia", color="blue")

#agregar etiquetas a la grafica
plt.xlabel(f"Analisis de {fecha}")
plt.ylabel("Total")
plt.grid(True)

#mostrar la grafica
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df= pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#confirmar que la columna fecha_inicio sea tipo datetime
df['fecha_inicio'] = pd.to_datetime(df["fecha_inicio"])

#establecer la columna 'fecha_inicio' como indice para aplicar a la time serie
df.set_index("fecha_inicio", inplace=True)

#se usa para hacer dinamico el titulo
variable_analizar ="costo_por_mil_impresiones" #cambiar cuanndo sea necesario

#seleccionar las columnas para analizar
df_time_series = df[variable_analizar]

#resamplear los datos segun sea necesario analizar Dia 'D',
#semana 'W' o año 'Y'
df_resampled = df_time_series.resample("D").sum()

#se usa para hacer dinamicas las etiquetas
if df_time_series.resample("D"):
    fecha="Dia"
elif df_time_series.resample("W"):
    fecha="Semana"
elif df_time_series.resample("M"):
    fecha="Mes"
else:
    fecha="Anual"

#Graficar la serie de tiempo
plt.figure(figsize=(10, 6))
df_resampled.plot(title=f"Serie de tiempo de {variable_analizar} por dia", color="blue")

#agregar etiquetas a la grafica
plt.xlabel(f"Analisis de {fecha}")
plt.ylabel("Total")
plt.grid(True)

#mostrar la grafica
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df= pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#confirmar que la columna fecha_inicio sea tipo datetime
df['fecha_inicio'] = pd.to_datetime(df["fecha_inicio"])

#establecer la columna 'fecha_inicio' como indice para aplicar a la time serie
df.set_index("fecha_inicio", inplace=True)

#se usa para hacer dinamico el titulo
variable_analizar ="costo_por_resultado" #cambiar cuanndo sea necesario

#seleccionar las columnas para analizar
df_time_series = df[variable_analizar]

#resamplear los datos segun sea necesario analizar Dia 'D',
#semana 'W' o año 'Y'
df_resampled = df_time_series.resample("D").sum()

#se usa para hacer dinamicas las etiquetas
if df_time_series.resample("D"):
    fecha="Dia"
elif df_time_series.resample("W"):
    fecha="Semana"
elif df_time_series.resample("M"):
    fecha="Mes"
else:
    fecha="Anual"

#Graficar la serie de tiempo
plt.figure(figsize=(10, 6))
df_resampled.plot(title=f"Serie de tiempo de {variable_analizar} por dia", color="blue")

#agregar etiquetas a la grafica
plt.xlabel(f"Analisis de {fecha}")
plt.ylabel("Total")
plt.grid(True)

#mostrar la grafica
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df= pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#confirmar que la columna fecha_inicio sea tipo datetime
df['fecha_inicio'] = pd.to_datetime(df["fecha_inicio"])

#establecer la columna 'fecha_inicio' como indice para aplicar a la time serie
df.set_index("fecha_inicio", inplace=True)

#se usa para hacer dinamico el titulo
variable_analizar ="click_enlace_trafico" #cambiar cuanndo sea necesario

#seleccionar las columnas para analizar
df_time_series = df[variable_analizar]

#resamplear los datos segun sea necesario analizar Dia 'D',
#semana 'W' o año 'Y'
df_resampled = df_time_series.resample("D").sum()

#se usa para hacer dinamicas las etiquetas
if df_time_series.resample("D"):
    fecha="Dia"
elif df_time_series.resample("W"):
    fecha="Semana"
elif df_time_series.resample("M"):
    fecha="Mes"
else:
    fecha="Anual"

#Graficar la serie de tiempo
plt.figure(figsize=(10, 6))
df_resampled.plot(title=f"Serie de tiempo de {variable_analizar} por dia", color="blue")

#agregar etiquetas a la grafica
plt.xlabel(f"Analisis de {fecha}")
plt.ylabel("Total")
plt.grid(True)

#mostrar la grafica
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#confirmar que la columna fecha_inicio sea tipo datetime
df['fecha_inicio'] = pd.to_datetime(df["fecha_inicio"])

#establecer la columna 'fecha_inicio' como indice para aplicar a la time serie
df.set_index("fecha_inicio", inplace=True)

#se usa para hacer dinamico el titulo
variable_analizar = "interaccion_post" # cambiar cuando sea necesario

#seleccionar las columnas para analizar
df_time_series =df[variable_analizar]

#resamplear los datos segun sea necesario analizar Dia 'D', semana 'W' o año 'Y'
df_resampled = df_time_series.resample("D").sum()

#se usa para hacere dinamicas las etiquetas
if df_time_series.resample("D"):
    fecha="Dia"
elif df_time_series.resample("W"):
    fecha="Semana"
elif df_time_series.resample("M"):
    fecha="Mes"
else:
    fecha="Anual"

#Graficar la serie de tiempo
plt.figure(figsize=(10, 6))
df_resampled.plot(title=f"Serie de tiempo de {variable_analizar} por dia", color="blue")

#agregar etiquetas a la grafica
plt.xlabel(f"Analisis de {fecha}")
plt.ylabel("Total")
plt.grid(True)

#mostrar la grafica
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df= pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#confirmar que la columna fecha_inicio sea tipo datatime a la time serie
df['fecha_inicio'] = pd.to_datetime(df["fecha_inicio"])

#establezco la columna "fecha_inicio" como el indice del dataframe para aplicar
df.set_index("fecha_inicio", inplace=True)

#se usa para hacer dinamico el titulo
variable_analizar ="like" #cambiar cuando sea necesario

#seleccionar las columnas para analizar 
df_time_series = df[variable_analizar]

#resamplear los datos segun sea necesario analizar ya sea Dia 'D', semana 'W', mes 'M'
#año 'Y'
df_resampled = df_time_series.resample("D").sum()

#se usa para hacer dinamicas las etiquetas
if df_time_series.resample("D"):
    fecha="Dia"
elif df_time_series.resample("W"):
    fecha="Semana"
elif df_time_series.resample("M"):
    fecha="Mes"
else:
    fecha="Anual"

#Graficar la serie de tiempo
plt.figure(figsize=(10,6))
df_resampled.plot(title=f"Serie de tiempo de {variable_analizar} por dia", color="blue")

#agregar etiquetas a grafica
plt.xlabel(f"Analisis {fecha}")
plt.ylabel("Total")
plt.grid(True)

#mostrar la grafica 
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df= pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#confirmar que la columna fecha_inicio sea tipo datetime
df['fecha_inicio'] = pd.to_datetime(df["fecha_inicio"])

#establecer la columna 'fecha_inicio' como indice para aplicar a la time serie
df.set_index("fecha_inicio", inplace=True)

#se usa para hacer dinamico el titulo
variable_analizar ="reacciones_post" #cambiar cuanndo sea necesario

#seleccionar las columnas para analizar
df_time_series = df[variable_analizar]

#resamplear los datos segun sea necesario analizar Dia 'D',
#semana 'W' o año 'Y'
df_resampled = df_time_series.resample("D").sum()

#se usa para hacer dinamicas las etiquetas
if df_time_series.resample("D"):
    fecha="Dia"
elif df_time_series.resample("W"):
    fecha="Semana"
elif df_time_series.resample("M"):
    fecha="Mes"
else:
    fecha="Anual"

#Graficar la serie de tiempo
plt.figure(figsize=(10, 6))
df_resampled.plot(title=f"Serie de tiempo de {variable_analizar} por dia", color="blue")

#agregar etiquetas a la grafica
plt.xlabel(f"Analisis de {fecha}")
plt.ylabel("Total")
plt.grid(True)

#mostrar la grafica
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df= pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#confirmar que la columna fecha_inicio sea tipo datetime
df['fecha_inicio'] = pd.to_datetime(df["fecha_inicio"])

#establecer la columna 'fecha_inicio' como indice para aplicar a la time serie
df.set_index("fecha_inicio", inplace=True)

#se usa para hacer dinamico el titulo
variable_analizar ="reacciones_post" #cambiar cuanndo sea necesario

#seleccionar las columnas para analizar
df_time_series = df[variable_analizar]

#resamplear los datos segun sea necesario analizar Dia 'D',
#semana 'W' o año 'Y'
df_resampled = df_time_series.resample("D").sum()

#se usa para hacer dinamicas las etiquetas
if df_time_series.resample("D"):
    fecha="Dia"
elif df_time_series.resample("W"):
    fecha="Semana"
elif df_time_series.resample("M"):
    fecha="Mes"
else:
    fecha="Anual"

#Graficar la serie de tiempo
plt.figure(figsize=(10, 6))
df_resampled.plot(title=f"Serie de tiempo de {variable_analizar} por dia", color="blue")

#agregar etiquetas a la grafica
plt.xlabel(f"Analisis de {fecha}")
plt.ylabel("Total")
plt.grid(True)

#mostrar la grafica
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df=pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#confirmar que la columna fecha_inicio sea tipo datatime
df['fecha_inicio'] = pd.to_datetime(df["fecha_inicio"])

#establecer la columna 'fecha_incio' para el indice para aplicar la time serie
df.set_index("fecha_inicio", inplace=True)

#se usa para hacer dinamico el titulo
variable_analizar = "reacciones_post" #cambiar cuando sea necesario

#selecccionar las columnas para analizar
df_time_series = df[variable_analizar]

#resamplear los datos segun sea necesario analizar ya sea Dia 'D',
#semana 'W' o año 'Y'
df_resampled = df_time_series.resample("D").sum()

#se usa para hacer dinamicas las etiquetas
if df_time_series.resample("D"):
    fecha="Dia"
elif df_time_series.resample("W"):
    fecha="Semana"
elif df_time_series.resample("M"):
    fecha="Mes"
else:
    fecha="Anual"

#Graficar la serie de tiempo
plt.figure(figsize=(10,6))
df_resampled.plot(title= f"Serie de tiempo de {variable_analizar}por dia", color="blue")

#agregar etiquetas a grafica
plt.xlabel(f"Analisis {fecha}")
plt.ylabel("Total")
plt.grid(True)

#mostrar la grafica
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#confirmar que la columna fecha_inicio sea tipo datetime
df['fecha_inicio'] = pd.to_datetime(df["fecha_inicio"])

#establecer la columna 'fecha_inicio' como el indice para aplicar a la
#time serie
df.set_index("fecha_inicio", inplace=True)

#se usa para hacer dinamico el titulo
variable_analizar = "veces_mostrado" #cambiar cuando sea necesario

#seleccionar las columnas para analizar
df_time_series = df[variable_analizar]

#resamplear los datos segun sea necesario analizar ya sea Dia 'D'
#semana 'W' o año 'Y'
df_resampled= df_time_series.resample("D").sum()

#se usa para hacer dinamicas las etiquetas
if df_time_series.resample("D"):
    fecha="Dia"
elif df_time_series.resample("W"):
    fecha="Semana"
elif df_time_series.resample("M"):
    fecha="Mes"
else:
    fecha="Anual"

#Graficar la serie de tiempo
plt.figure(figsize=(10, 6))
df_resampled.plot(title=f"Serie de tiempo de {variable_analizar}por dia", color="blue")

#agregar etiquetas a graficar
plt.xlabel(f"Analis {fecha}")
plt.ylabel("Total")
plt.grid(True)

#mostrar la grafica
plt.tight_layout()
plt.show()



# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

#cargamos el archivo cvs 
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#confirmar que la columna fecha_inicio sea tipo datetime
df['fecha_inicio'] = pd.to_datetime(df["fecha_inicio"])

#establecer la columna 'fecha_inicio' para el indice para aplicar la time serie
df.set_index("fecha_inicio", inplace=True)

#se usa para hacer dinamico el titulo
variable_analizar = "reacciones_post" #cambiar cuando sea necesario

#seleccionar las columnas para analizar
df_time_series = df[variable_analizar]

#resamplear los datos segun sea necesario analizar ya sea Dia 'D'
#semana 'W' o año 'Y'
df_resampled = df_time_series.resample("D").sum()

#se usa para hacer dinamicas las etiquetas
if df_time_series.resample("D"):
    fecha = "Dia"
elif df_time_series.resample("W"):
    fecha = "Semana"
elif df_time_series.resampe("M"):
    fecha = "Mes"
else:
    fecha = "Anual"

#Graficar la serie de tiempo 
plt.figure(figsize=(10,6))
df_resampled.plot(title = f"Serie de tiempo de {variable_analizar} por dia", color="blue")

#agregar etiquetas a grafica
plt.xlabel(f"Analisis {fecha}")
plt.ylabel("Total")
plt.grid(True)

#Mostrar la grafica 
plt.tight_layout()
plt.show()
