# Databricks notebook source
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

#cargar el csv 
df=pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#variable para analizar 
variable_analizar = 'gasto'# se cambia la variable para analizar

#crear grafica de densidad
plt.figure(figsize=(10, 6))
sns.kdeplot(df[variable_analizar], fill=True)

#etiquetas y titulo de la grafica 
plt.title(f"Densidad de {variable_analizar}")
plt.xlabel("Valor")
plt.ylabel("Densidad")

#invertvalos de 100 en 100
#plt.xticks(range(0, int(df[variable_analizar].max()+100,100)))
plt.tight_layout()
plt.show()


# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

#cargar el csv 
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#variable para analizarla
variable_analizar = "costo_por_mil_impresiones" #cambiarla para analizar otras

#creo grafica de densidad
plt.figure(figsize=(10, 6))
sns.kdeplot(df[variable_analizar], fill=True)

#etiquetas y titulos de la grafica 
plt.title(f"Densidad de {variable_analizar}")
plt.xlabel("Valor")
plt.ylabel("Densidad")

#intervalos de 100 en 100
plt.xticks(range(0, int(df[variable_analizar].max())+50,50))
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

#cargar el csv
df=pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#variable para analizar
variable_analizar = 'click_link' #se cambia la variable para analizar

#creo grafica de densidad
plt.figure(figsize=(10, 6))
sns.kdeplot(df[variable_analizar],fill=True)

#etiquetas y titulo de la grafica
plt.title(f"Densidad de {variable_analizar}")
plt.xlabel("Valor")
plt.ylabel("Densidad")

#intervalos de 100 en 100
#plt.xticks(range(0, int(df[variable_analizar].max())+ 100, 100))
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

#cargar el csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#variable para analizar
variable_analizar= 'vistas_video'

#creo grafica de densidad
plt.figure(figsize=(10, 6))
sns.kdeplot(df[variable_analizar], fill=True)

#etiquetas y titulo de la grafica
plt.title(f"Densidad de {variable_analizar}")
plt.xlabel("Valor")
plt.ylabel("Densidad")

#intervalos de 100 en 100
plt.xticks(range(0, int(df[variable_analizar].max())+500,500))
plt.tight_layout()
plt.show()


# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

#cargar el csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#variable para analizar
variable_analizar = 'veces_mostrado'

#creo grafica de densidad
plt.figure(figsize=(10,6))
sns.kdeplot(df[variable_analizar],fill=True)

#etiquetas y titulo de la grafica
plt.title(f"Densidad de {variable_analizar}")
plt.xlabel("Valor")
plt.ylabel("Densidad")

#intervalos de 100 en 100
#plt.xticks(range(0, int(df[variable_analizar].max())+100,100))
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

#cargar el csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#variable para analizar
variable_analizar='like'

#creo grafica de densidad
plt.figure(figsize=(10,6))
sns.kdeplot(df[variable_analizar],fill=True)

#etiquetas y titulo de la grafica
plt.title(f"Densidad de {variable_analizar}")
plt.xlabel("Valor")
plt.ylabel("Densidad")

#intervalo de 100 en 100
#plt.xticks(range(0, int(df[variable_analizar].max())+100,100))
plt.tight_layout()
plt.show()



# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

#cargar el csv
df=pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#variable para analizar
variable_analizar = 'promedio_frecuencia'

#creo grafica de densidad
plt.figure(figsize=(10, 6))
sns.kdeplot(df[variable_analizar], fill=True)

#etiquetas y titulo de la grafica 
plt.title(f"Densidad de {variable_analizar}")
plt.xlabel("Valor")
plt.ylabel("Densidad")

#etiquetas y titulo de la grafica
#plt.xticks(range(0, int(df[variable_analizar].max())+100,100))
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

#cargar el csv 
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#variable para analizar
variable_analizar = "personas_alcanzadas"

#creo grafica de densidad
plt.figure(figsize=(10, 6))
sns.kdeplot(df[variable_analizar],fill=True)

#etiquetas y titulo de la grafica
plt.title(f"Densidad de {variable_analizar}")
plt.xlabel("Valor")
plt.ylabel("Densidad")

#intervalos de 100 en 100
plt.xticks(range(0, int(df[variable_analizar].max())+2500,2500))
plt.tight_layout()
plt.show()


# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

#cargar el csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#variable para analizar
variable_analizar = "interaccion_post" #cambiar cuando sea necesario

#creo grafica de densidad 
plt.figure(figsize=(10, 6))
sns.kdeplot(df[variable_analizar], fill=True)

#etiquetas y titulo de la grafica 
plt.title(f"Densidad de {variable_analizar}")
plt.xlabel("Valor")
plt.ylabel("Densidad")

#intervalos de 100 en 100
plt.xticks(range(0, int(df[variable_analizar].max())+500,500))
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

#cargar el csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#variable para analizar
variable_analizar = 'costo_por_resultado'#cambiar cuando sea necesario

#creo grafica de densidad
plt.figure(figsize=(10, 6))
sns.kdeplot(df[variable_analizar], fill=True)

#etiquetas y titulo de la grafica
plt.title(f"Densidad de {variable_analizar}")
plt.xlabel("Valor")
plt.ylabel("Densidad")

#intervalos de 100 en 100
plt.xticks(range(0,int(df[variable_analizar].max())+50,50))
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

#cargar el csv
df=pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#variable para analizar
variable_analizar ='costo_por_click_anuncio'#se cambia la variable para analizar

#creo grafica de densidad 
plt.figure(figsize=(10,6))
sns.kdeplot(df[variable_analizar],fill=True)

#etiquetas y titulo de la grafica
plt.title(f"Densidad de {variable_analizar}")
plt.xlabel("Valor")
plt.ylabel("Densidad")

#Invervalos de 100 en 100
#plt.xticks(range(0, int(df[variable_analizar].max())+100,100))
plt.tight_layout()
plt.show()


# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

#cargar el csv
df=pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#variable para analizar
variable_analizar = "conversion_primer_respuesta" #se cambia cuando sea necesario

#creo la grafica de densidad
plt.figure(figsize=(10,6))
sns.kdeplot(df[variable_analizar], fill=True)

#etiquetas y titulo de la grafica
plt.title(f"Densidad de {variable_analizar}")
plt.xlabel("Valor")
plt.ylabel("Densidad")

#Intervalos de 100 en 100
#plt.xticks(range(0, int(df[variable_analizar].max())+100,100))
plt.tight_layout()
plt.show()




# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

#cargar el csv
df=pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#variable para analizar
variable_analizar ='conversion_boton_msj'#se cambia la variable para analizar

#crar grafica de densidad
plt.figure(figsize=(10, 6))
sns.kdeplot(df[variable_analizar], fill=True)

#etiquetas y titulo de la grafica
plt.title(f"Densidad de {variable_analizar}")
plt.xlabel("Valor")
plt.ylabel("Densidad")

#intervalos de 100 en 100
#plt.xticks(range(0, int(df[variable_analizar].max())+100,100))
plt.tight_layout()
plt.show()



# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

#cargar el csv
df=pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#variable para analizar 
variable_analizar='contenido_guardado' #se cambio la variable para analizar

#creo grafico de densidad
plt.figure(figsize=(10,6))
sns.kdeplot(df[variable_analizar], fill=True)

#etiqueta y titulo de la grafica 
plt.title(f"Densidad de {variable_analizar}")
plt.xlabel("Valor")
plt.ylabel("Densidad")

#intervalos de 100 en 100
#plt.xticks(range(0, int(df[variable_analizar].max()) + 100,100))
plt.tight_layout()
plt.show()


# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

#cargar el csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#variable para analizar
variable_analizar = 'comentarios'

#creo grafica de densidad
plt.figure(figsize=(10, 6))
sns.kdeplot(df[variable_analizar], fill=True)

#etiquetas y titulo de la grafica
plt.title(f"Densidad de {variable_analizar}")
plt.xlabel("Valor")
plt.ylabel("Densidad")

#intervalos de 100 en 100
#plt.xticks(range(0, int(df[variable_analizar].max())+100,100))
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd 
import matplotlib.pyplot as plt
import seaborn as sns

# cargar el csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

# variable para analizar
variable_analizar = 'clicks_en_anuncio' #se cambia la variable para analizar

# creo grafica de densidad
plt.figure(figsize=(10, 6))
sns.kdeplot(df[variable_analizar], fill=True)

# etiquetas y titulo de la grafica
plt.title(f"Densidad de {variable_analizar}")
plt.xlabel("Valor")
plt.ylabel("Densidad")

# intervalos de 100 en 100 
plt.xticks(range(0, int(df[variable_analizar].max()) + 100, 100))
plt.tight_layout()
plt.show()



# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

#cargar el csv
df= pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

#variable para analizar
variable_analizar ="click_enlace_trafico"#cambiarla cuando sea necesario

#crear grafica de densidad
plt.figure(figsize=(10,6))
sns.kdeplot(df[variable_analizar], fill=True)

#etiquetas y titulo de la grafica
plt.title(f"Densidad de {variable_analizar}")
plt.xlabel("Valor")
plt.ylabel("Densidad")

#intervalo de 100 en 100
plt.xticks(range(0, int (df[variable_analizar].max())+5,5))
plt.tight_layout()
plt.show()

# COMMAND ----------

import pandas as pd 
import matplotlib.pyplot as plt
import seaborn as sns

# cargar el csv
df = pd.read_csv("/dbfs/mnt/processed/Ads_General_Por_Dia/anuncios_insights_general_por_dia_limpiado.csv")

# variable para analizar
variable_analizar = 'clicks_en_anuncio' #se cambia la variable para analizar

# creo grafica de densidad
plt.figure(figsize=(10, 6))
sns.kdeplot(df[variable_analizar], fill=True)

# etiquetas y titulo de la grafica
plt.title(f"Densidad de {variable_analizar}")
plt.xlabel("Valor")
plt.ylabel("Densidad")

# intervalos de 100 en 100 
plt.xticks(range(0, int(df[variable_analizar].max()) + 50, 50))
plt.tight_layout()
plt.show()






