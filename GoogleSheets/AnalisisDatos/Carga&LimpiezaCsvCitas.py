# Databricks notebook source
# MAGIC %md
# MAGIC ## Instalar las librerias necesarias para hacer el manejo

# COMMAND ----------

# MAGIC %pip install gspread google-auth
# MAGIC %pip install gspread oauth2client
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## reiniciar las librerias para poder usarlas (siempre se tiene que hacer)

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## coneccion con el archivo en especifico para extraer el sheet(archivo) y worksheet(hojas dentro del archivo)

# COMMAND ----------

import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import os

# Configuración de credenciales
scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Ruta al archivo de credenciales JSON en DBFS
json_creds_path = "/dbfs/mnt/GoogleSheets/Credenciales/GoogleSheets_Credenciales.json"

# Cargar las credenciales con el alcance definido
creds = Credentials.from_service_account_file(json_creds_path, scopes=scopes)
client = gspread.authorize(creds)

# Introducir el ID del Google Sheet
sheet_id = "1ba6l4dFTroMgKk-zqI-jMBSEY4ZMgDJe7m3JrrtKXLE"  # Aquí debes colocar el ID de tu hoja, ejemplo: "1dBFzUMXbEMDSPuoyDOi6L2gGnD-fnK1dNt4h8ei_H0"

# Ruta base donde se guardarán los archivos CSV
base_path = "/dbfs/mnt/GoogleSheets/CSV's_Descargados/"

# Crear el directorio si no existe
if not os.path.exists(base_path):
    os.makedirs(base_path)
    print(f"Directorio creado: {base_path}")
else:
    print(f"Directorio ya existente: {base_path}")

# Conexión al Google Sheet por ID
try:
    spreadsheet = client.open_by_key(sheet_id)
    print(f"Conexión exitosa al documento: {spreadsheet.title}")
    
    # Descargar todas las hojas (worksheets) existentes
    worksheets = spreadsheet.worksheets()
    print("Hojas de cálculo disponibles:")
    for sheet in worksheets:
        print(f"- {sheet.title}")
        # Descargar todos los registros de la hoja
        data = sheet.get_all_records()  # Descarga los datos como una lista de diccionarios
        df = pd.DataFrame(data)  # Convertir los datos a un DataFrame de pandas
        print(f"\nDatos de la hoja '{sheet.title}':")
        print(df.head())  # Mostrar las primeras filas para confirmar

        # Guardar cada hoja como archivo CSV en DBFS
        file_path = os.path.join(base_path, f"{sheet.title}.csv")
        df.to_csv(file_path, index=False)
        print(f"Datos de la hoja '{sheet.title}' guardados en: {file_path}")

except Exception as e:
    print(f"Error al conectar o procesar las hojas de cálculo: {e}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### revisar que se extrajo en orden

# COMMAND ----------

display(dbutils.fs.ls("/mnt/GoogleSheets/CSV's_Descargados/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##lee cuantos registros contiene cada archivo

# COMMAND ----------

import os
import pandas as pd

# Ruta de la carpeta donde están los archivos
folder_path = "/dbfs/mnt/GoogleSheets/CSV's_Descargados/"

# Inicializar contadores
total_records = 0
file_records = {}

# Recorrer todos los archivos en la carpeta
for file_name in os.listdir(folder_path):
    file_path = os.path.join(folder_path, file_name)
    
    # Verificar si es un archivo (y no una carpeta)
    if os.path.isfile(file_path):
        try:
            # Procesar solo archivos CSV o TXT (puedes agregar más formatos si es necesario)
            if file_name.endswith(".csv"):
                # Leer el archivo CSV
                df = pd.read_csv(file_path)
                records = len(df)
                file_records[file_name] = records
                total_records += records
            elif file_name.endswith(".txt"):
                # Leer archivo de texto línea por línea
                with open(file_path, "r") as f:
                    records = sum(1 for line in f)
                file_records[file_name] = records
                total_records += records
        except Exception as e:
            print(f"Error procesando el archivo {file_name}: {e}")

# Mostrar los resultados
print("Cantidad de registros por archivo:")
for file, count in file_records.items():
    print(f"{file}: {count} registros")

print(f"\nCantidad total de registros en la carpeta: {total_records}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## eliminar columnas que no se necesitan

# COMMAND ----------

import pandas as pd
import os

# Ruta donde están los CSVs
folder_path = "/dbfs/mnt/GoogleSheets/CSV's_Descargados/"

# Iterar sobre los archivos en la carpeta
for file in os.listdir(folder_path):
    if file.endswith(".csv"):  # Verifica que sea un archivo CSV
        file_path = os.path.join(folder_path, file)
        
        # Leer el archivo CSV
        df = pd.read_csv(file_path)
        
        # Filtrar las columnas eliminando las que se llamen 'Unnamed', 'landings' o 'Día'
        columns_to_remove = [col for col in df.columns if "Unnamed" in col or col in ["landings", "Día","dia"]]
        df = df.drop(columns=columns_to_remove, errors="ignore")
        
        # Sobrescribir el archivo original con el DataFrame limpio
        df.to_csv(file_path, index=False)
        print(f"Archivo limpiado y sobrescrito: {file_path}")

print("Proceso de limpieza completado.")




# COMMAND ----------

# MAGIC %md
# MAGIC ## cambio de columnas a minusculas y sin acentos

# COMMAND ----------

import pandas as pd
import os
import unicodedata

# Ruta donde están los CSVs
folder_path = "/dbfs/mnt/GoogleSheets/CSV's_Descargados/"

# Función para eliminar acentos de los nombres de las columnas
def remove_accents(column_name):
    return ''.join(
        char for char in unicodedata.normalize('NFD', column_name)
        if unicodedata.category(char) != 'Mn'
    )

# Iterar sobre los archivos en la carpeta
for file in os.listdir(folder_path):
    if file.endswith(".csv"):  # Verifica que sea un archivo CSV
        file_path = os.path.join(folder_path, file)
        
        # Leer el archivo CSV
        df = pd.read_csv(file_path)
        
        # Convertir columnas a minúsculas y eliminar acentos
        df.columns = [remove_accents(col.lower()) for col in df.columns]
        
        # Sobrescribir el archivo original con el DataFrame modificado
        df.to_csv(file_path, index=False)
        print(f"Archivo actualizado: {file_path}")

print("Proceso completado: columnas en minúsculas y sin acentos.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## renombrar columnas de todos los archivos
# MAGIC

# COMMAND ----------

import pandas as pd
import os

# Ruta donde están los CSVs
folder_path = "/dbfs/mnt/GoogleSheets/CSV's_Descargados/"

# Iterar sobre los archivos en la carpeta
for file in os.listdir(folder_path):
    if file.endswith(".csv"):  # Verifica que sea un archivo CSV
        file_path = os.path.join(folder_path, file)
        
        # Leer el archivo CSV
        df = pd.read_csv(file_path)
        
        # Renombrar columnas según el mapeo
        df.rename(columns={
            "nombre": "cliente",
            "ad": "anuncio",
            "fecha": "fecha_cita",
            "hora": "hora_cita",
            "telefono": "contacto_cliente",
        }, inplace=True)
        
        # Guardar el archivo actualizado en el mismo lugar
        df.to_csv(file_path, index=False)
        print(f"Archivo procesado y guardado: {file_path}")

print("Renombrado de columnas completado.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## establecer tipos de columnas

# COMMAND ----------

import pandas as pd
import os

# Ruta donde están los CSVs
folder_path = "/dbfs/mnt/GoogleSheets/CSV's_Descargados/"

# Iterar sobre los archivos en la carpeta
for file in os.listdir(folder_path):
    if file.endswith(".csv"):  # Verifica que sea un archivo CSV
        file_path = os.path.join(folder_path, file)
        
        # Leer el archivo CSV
        df = pd.read_csv(file_path)

        # Convertir columnas a los tipos de datos deseados
        df['cliente'] = df['cliente'].astype(str)
        df['medio'] = df['medio'].astype(str)
        df['anuncio'] = df['anuncio'].astype(str)  # Opcional, ya que está vacío
        df['fecha_cita'] = pd.to_datetime(df['fecha_cita'], format='%d/%m/%Y', errors='coerce')  # Convertir a fecha
        df['hora_cita'] = pd.to_datetime(df['hora_cita'], format='%H:%M', errors='coerce').dt.time  
        df['fraccionamiento'] = df['fraccionamiento'].astype(str)
        df['asesor'] = df['asesor'].astype(str)
        df['contacto_cliente'] = df['contacto_cliente'].astype(str)  # Mantener como texto para evitar problemas

        # Sobrescribir el archivo original con el DataFrame formateado
        df.to_csv(file_path, index=False)
        print(f"Archivo formateado y sobrescrito: {file_path}")

print("Proceso de cambio de tipos realizado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## combinar todos los archivos en uno solo

# COMMAND ----------

import pandas as pd
import os

# Ruta donde están los CSVs
folder_path = "/dbfs/mnt/GoogleSheets/CSV's_Descargados/"

# Lista para almacenar los DataFrames
dataframes = []

# Iterar sobre los archivos en la carpeta
for file in os.listdir(folder_path):
    if file.endswith(".csv"):  # Verifica que sea un archivo CSV
        file_path = os.path.join(folder_path, file)
        
        # Leer el archivo CSV
        df = pd.read_csv(file_path)
        
        # Agregar el DataFrame a la lista
        dataframes.append(df)

# Combinar los DataFrames por columnas comunes
combined_df = pd.concat(dataframes, axis=0, join="inner", ignore_index=True)

# Guardar el DataFrame combinado en un nuevo archivo
output_path = os.path.join(folder_path, "Combined_Data.csv")
combined_df.to_csv(output_path, index=False)

print(f"Se han combinado todos los archivos en: {output_path}")



# COMMAND ----------

# MAGIC %md
# MAGIC ## revisar que esten los registros de todos los archivos combinados

# COMMAND ----------

import pandas as pd

df=pd.read_csv("/dbfs/mnt/GoogleSheets/CSV's_Descargados/Combined_Data.csv")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## cambia a minusculas el contenido de medio, asesor y fraccionamiento (para analizar mejor en grupo)

# COMMAND ----------

import pandas as pd

# Ruta al archivo CSV específico
file_path = "/dbfs/mnt/GoogleSheets/CSV's_Descargados/Combined_Data.csv"

# Leer el archivo CSV
try:
    df = pd.read_csv(file_path)

    # Convertir todos los valores de las columnas 'medio' y 'asesor' a minúsculas
    df['medio'] = df['medio'].str.lower()
    df['asesor'] = df['asesor'].str.lower()
    df['fraccionamiento'] = df['fraccionamiento'].str.lower()

    # Sobrescribir el archivo original con los cambios
    df.to_csv(file_path, index=False)
    print(f"Valores de las columnas 'medio' y 'asesor' convertidos a minúsculas y guardados en: {file_path}")

except Exception as e:
    print(f"Error al procesar el archivo: {e}")

print("Cambio completado de contenido en columnas a minusculas.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Regularizacion de variables en columnas para facilitar el analisis (para informacion escrita de muchas formas con el mismo significado)

# COMMAND ----------

import pandas as pd

# Ruta al archivo CSV específico
file_path = "/dbfs/mnt/GoogleSheets/CSV's_Descargados/Combined_Data.csv"

# Leer el archivo CSV
try:
    df = pd.read_csv(file_path)

    # Reemplazar valores específicos en la columna 'medio'
    replacements = {
        r'\bface\b': 'facebook',  # Cambiar 'face' por 'facebook'
        r'\bpag\s*web\b': 'pagina web',  # Cambiar 'pag web' por 'pagina web'
        r'\bwhats\b': 'whatsapp'  # Cambiar 'whats' por 'whatsapp'
    }

    for pattern, replacement in replacements.items():
        df['medio'] = df['medio'].str.replace(pattern, replacement, case=False, regex=True)

    # Sobrescribir el archivo original con los cambios
    df.to_csv(file_path, index=False)
    print(f"Valores actualizados en la columna 'medio' y guardados en: {file_path}")
    print("Proceso de reemplazo con normalizacion completado.")

except Exception as e:
    print(f"Error al procesar el archivo: {e}")




# COMMAND ----------

import pandas as pd

df = pd.read_csv("/dbfs/mnt/GoogleSheets/CSV's_Descargados/Combined_Data.csv")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## quitar espacios en blanco y cambiarlos por '_' guiones bajos

# COMMAND ----------

import pandas as pd

# Ruta del archivo específico
file_path = "/dbfs/mnt/GoogleSheets/CSV's_Descargados/Combined_Data.csv"

# Leer el archivo CSV
df = pd.read_csv(file_path)

# Lista de columnas a procesar
columns_to_process = ['cliente', 'medio', 'anuncio', 'fraccionamiento', 'asesor']

# Limpiar espacios y reemplazar espacios entre palabras por guiones bajos
for column in columns_to_process:
    df[column] = df[column].apply(
        lambda x: '_'.join(x.strip().split()) if isinstance(x, str) else x
    )

# Guardar los cambios sobrescribiendo el archivo original
df.to_csv(file_path, index=False)

print(f"Archivo actualizado con espacios limpiados y reemplazados por guiones bajos: {file_path}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Opcional para mejorar el analisis
# MAGIC
# MAGIC import pandas as pd
# MAGIC
# MAGIC # Ruta al archivo CSV específico
# MAGIC file_path = "/dbfs/mnt/GoogleSheets/CSV's_Descargados/CitasCombinadas_Rename.csv"
# MAGIC
# MAGIC # Leer el archivo CSV
# MAGIC try:
# MAGIC     df = pd.read_csv(file_path)
# MAGIC
# MAGIC     # Contar las palabras en la columna 'fraccionamiento'
# MAGIC     count_values = df['fraccionamiento'].value_counts()
# MAGIC
# MAGIC     # Mostrar los resultados
# MAGIC     print("Conteo de valores en la columna 'medio':")
# MAGIC     print(count_values)
# MAGIC
# MAGIC     # Opcional: Guardar el conteo en un archivo CSV
# MAGIC     count_values.to_csv("/dbfs/mnt/GoogleSheets/CSV's_Descargados/Conteo_Medio.csv", header=["Conteo"])
# MAGIC     print("Conteo guardado en: /dbfs/mnt/GoogleSheets/CSV's_Descargados/Conteo_Asesor.csv")
# MAGIC
# MAGIC except Exception as e:
# MAGIC     print(f"Error al procesar el archivo: {e}")
# MAGIC
# MAGIC print("Proceso de conteo completado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## limpieza de valores en columna para mejorar el analisis

# COMMAND ----------

import pandas as pd

# Ruta al archivo CSV específico
file_path = "/dbfs/mnt/GoogleSheets/CSV's_Descargados/Combined_Data.csv"

# Leer el archivo CSV
try:
    df = pd.read_csv(file_path)

    # Reemplazar los valores que contengan 'crisina' o 'cristi' con 'cristina' en la columna 'asesor'
    df['asesor'] = df['asesor'].str.replace(r'\b(crisina|cristi)\b', 'cristina', case=False, regex=True)

    # Sobrescribir el archivo original con los cambios
    df.to_csv(file_path, index=False)
    print(f"Valores actualizados en la columna 'asesor' y guardados en: {file_path}")
    print("Proceso de reemplazo completado.")

except Exception as e:
    print(f"Error al procesar el archivo: {e}")




# COMMAND ----------

import pandas as pd

pd = pd.read_csv("/dbfs/mnt/GoogleSheets/CSV's_Descargados/Combined_Data.csv")

display(pd)

# COMMAND ----------

# MAGIC %md
# MAGIC ## crear vista del csv total de citas para analisis en powerbi
