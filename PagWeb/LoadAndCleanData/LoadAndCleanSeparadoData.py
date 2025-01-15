# Databricks notebook source
#GET - https://api.casaspecsa.com/api/venta/all-transacciones
#SI QUIERE TRAER SOLO LAS TRANSACCIONES YA SEA VENDIDAS, SEPARADAS(O INICIADAS) O CANCELADAS AGREGAR estatus COMO VARIABLE 

#valores aceptados: 
#<"Vendido" || "Cancelado" || "Separado" || "Iniciado">

#ejemplo:
#GET - https://api.casaspecsa.com/api/venta/all-transacciones?estatus=Vendido

#se extrae solo la data relacionada a pagos y separaciones para hacer un analisis mas minucioso
import requests
import pandas as pd
import os

#URL base de la api(traer toda la info existente)
BASE_URL ="https://api.casaspecsa.com/api/venta/all-transacciones?estatus=Separado"

#Parametros iniciales
limit = 10 #numero de registros que se taera por pagina
page=1 #inicio de pagina
all_data=[] #Lista para guardar toda la data

# Ruta de salida en Databricks
output_dir = "/dbfs/mnt/PagWeb/Extract/Separados"
output_file = os.path.join(output_dir, "separados_web.csv")

# Crear la ruta si no existe
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
    print(f"Ruta creada: {output_dir}")
else:
    print(f"Ruta ya existe: {output_dir}")

# Función para extraer datos con paginación
while True:
    # Crear la URL con parámetros
    params = {"limit": limit, "page": page}
    
    try:
        # Solicitud a la API
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()  # Verifica errores en la respuesta
        
        # Extraer datos en formato JSON
        data = response.json()
        
        # Verificar si hay datos en la página actual
        if not data or len(data) == 0:
            print("No hay más datos disponibles.")
            break
        
        # Agregar los datos al listado general
        all_data.extend(data)
        print(f"Página {page} procesada con éxito. Registros obtenidos: {len(data)}")
        
        # Incrementar el número de página
        page += 1
    
    except requests.exceptions.RequestException as e:
        print(f"Error al conectar con la API: {e}")
        break

# Convertir los datos a un DataFrame de pandas
df = pd.DataFrame(all_data)

# Guardar los datos en un archivo CSV en Databricks
df.to_csv(output_file, index=False)
print(f"Datos exportados a {output_file}")



# COMMAND ----------

import pandas as pd

df= pd.read_csv("/dbfs/mnt/PagWeb/Extract/Separados/separados_web.csv")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Renombrar columnas para evitar tener problemas en columnas iguales al abrir diccionarios

# COMMAND ----------

#codigo para cambiar el nombre de las columnnas ya que se repiten varios
import pandas as pd

#leer el csv desde la ruta especificada
df = pd.read_csv("/dbfs/mnt/PagWeb/Extract/Separados/separados_web.csv")

#diccionario con los cambios de nombres en columnas/agregar nombres columnas a cambiar por nuevos nombres
new_column_names ={
    'id': 'id_gral',
    'folio': 'folio_gral'
    # Agrega todos los nombres de columnas que deseas cambiar
}

# Renombrar las columnas
df.rename(columns=new_column_names, inplace=True)

# Guardar el DataFrame renombrado en un nuevo archivo CSV
df.to_csv("/dbfs/mnt/PagWeb/Extract/Separados/separados_web_renombradoColumns.csv", index=False)

print("Archivo renombrado y guardado exitosamente.")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expandir la columna cliente para abrir el diccionario

# COMMAND ----------

import pandas as pd

# Función para expandir la columna 'cliente'
def expand_cliente(cliente):
    try:
        # Validar si el valor ya es un diccionario
        if isinstance(cliente, dict):
            cliente_dict = cliente  # Ya es un diccionario, no se necesita convertir
        elif isinstance(cliente, str) and cliente.strip():
            cliente_dict = eval(cliente)  # Convertir cadena a diccionario
        else:
            return pd.Series({'error': 'Valor nulo o inválido'})  # Registrar el error
        
        # Normalizar datos anidados
        expanded = pd.json_normalize(cliente_dict)
        return expanded.iloc[0]  # Devolver la primera fila como Serie
    except Exception as e:
        print(f"Error al procesar el cliente: {cliente} - {e}")
        return pd.Series({'error': 'Error de procesamiento'})  # Registrar el error

# Aplicar la función para expandir la columna 'cliente'
df_expanded = df['cliente'].apply(expand_cliente)

# Concatenar las nuevas columnas con el DataFrame original
df_final = pd.concat([df.drop('cliente', axis=1), df_expanded], axis=1)

# Guardar el resultado en un nuevo archivo CSV
output_path = "/dbfs/mnt/PagWeb/Extract/Separados/separados_web_clienteExpanded.csv"
df_final.to_csv(output_path, index=False)
print(f"Datos expandidos guardados en: {output_path}")

# Mostrar los resultados
display(df_final)




# COMMAND ----------

# MAGIC %md
# MAGIC ## cambiar columnas que inician con 'datos.' por 'cliente_'

# COMMAND ----------

import pandas as pd

# Leer el CSV desde la ruta especificada
df = pd.read_csv("/dbfs/mnt/PagWeb/Extract/Separados/separados_web_clienteExpanded.csv")

# Reemplazar las columnas que empiezan con 'datos.' por 'cliente_'
df.columns = [col.replace("datos.", "cliente_") if col.startswith("datos.") else col for col in df.columns]

# Guardar el DataFrame renombrado en un nuevo archivo CSV
df.to_csv("/dbfs/mnt/PagWeb/Extract/Separados/separados_web_renombradoColumnsCliente.csv", index=False)

print("Archivo renombrado y guardado exitosamente.")
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Se agrega un sufijo '_' a las columnas expandidas de lote y en caso de que existan anidaciones se agrega el nombre de la columna padre como prefijo a las columnas anidades

# COMMAND ----------

import pandas as pd

# Leer archivo CSV
df = pd.read_csv("/dbfs/mnt/PagWeb/Extract/Separados/separados_web_renombradoColumnsCliente.csv")

# Función para expandir la columna 'lote'
def expand_lote(lote):
    try:
        # Validar si el valor ya es un diccionario
        if isinstance(lote, dict):
            lote_dict = lote
        elif isinstance(lote, str) and lote.strip():
            lote_dict = eval(lote)#eval convierte una cadena con formato de diccionario(como JSON en python), en un verdadero dicc de py
        else:
            return pd.Series({'error': 'Valor nulo o inválido'})
        
        # Normalizar datos anidados y agregar prefijo del nombre de la columna
        expanded = pd.json_normalize(lote_dict, sep='_')
        expanded.columns = [f"lote_{col}" for col in expanded.columns]  # Agregar prefijo 'lote_' a todas las columnas anidadas
        return expanded.iloc[0]
    except Exception as e:
        print(f"Error al procesar el lote: {lote} - {e}")
        return pd.Series({'error': 'Error de procesamiento'})



# Aplicar la función para expandir la columna 'lote'
df_expanded = df['lote'].apply(expand_lote)

# Concatenar con el DataFrame original y agregar las columnas expandidas al final
df_final = pd.concat([df.drop('lote', axis=1), df_expanded], axis=1)

# Guardar el archivo resultante
output_path = "/dbfs/mnt/PagWeb/Extract/Separados/separados_web_loteExpanded.csv"
df_final.to_csv(output_path, index=False)
print(f"Datos expandidos guardados en: {output_path}")


# Mostrar los resultados
display(df_final)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Se agrega un sufijo '_' a las columnas expandidas de estatus y en caso de que existan anidaciones se agrega el nombre de la columna padre como prefijo a las columnas anidades

# COMMAND ----------

import pandas as pd

# Leer archivo CSV
df = pd.read_csv("/dbfs/mnt/PagWeb/Extract/Separados/separados_web_loteExpanded.csv")

# Función para expandir la columna 'estatus'
def expand_estatus(estatus):
    try:
        # Validar si el valor ya es un diccionario
        if isinstance(estatus, dict):
            estatus_dict = estatus
        elif isinstance(estatus, str) and estatus.strip():
            estatus_dict = eval(estatus)#eval convierte una cadena con formato de diccionario(como JSON en python), en un verdadero dicc de py
        else:
            return pd.Series({'error': 'Valor nulo o inválido'})
        
        # Normalizar datos anidados y agregar prefijo del nombre de la columna
        expanded = pd.json_normalize(estatus_dict, sep='_')
        expanded.columns = [f"estatus_{col}" for col in expanded.columns]  # Agregar prefijo 'lote_' a todas las columnas anidadas
        return expanded.iloc[0]
    except Exception as e:
        print(f"Error al procesar el estatus: {estatus} - {e}")
        return pd.Series({'error': 'Error de procesamiento'})
    

# Aplicar la función para expandir la columna 'estatus'
df_expanded = df['estatus'].apply(expand_estatus)

# Concatenar con el DataFrame original y agregar las columnas expandidas al final
df_final = pd.concat([df.drop('estatus', axis=1), df_expanded], axis=1)

# Guardar el archivo resultante
output_path = "/dbfs/mnt/PagWeb/Extract/Separados/separados_web_estatusExpanded.csv"
df_final.to_csv(output_path, index=False)
print(f"Datos expandidos guardados en: {output_path}")


# Mostrar los resultados
display(df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Se agrega un sufijo '_' a las columnas expandidas de asesor y en caso de que existan anidaciones se agrega el nombre de la columna padre como prefijo a las columnas anidades

# COMMAND ----------

import pandas as pd

# Leer archivo CSV
df = pd.read_csv("/dbfs/mnt/PagWeb/Extract/Separados/separados_web_estatusExpanded.csv")

# Función para expandir la columna 'asesor'
def expand_asesor(asesor):
    try:
        # Validar si el valor ya es un diccionario
        if isinstance(asesor, dict):
            asesor_dict = asesor
        elif isinstance(asesor, str) and asesor.strip():
            asesor_dict = eval(asesor)#eval convierte una cadena con formato de diccionario(como JSON en python), en un verdadero dicc de py
        else:
            return pd.Series({'error': 'Valor nulo o inválido'})
        
        # Normalizar datos anidados y agregar prefijo del nombre de la columna
        expanded = pd.json_normalize(asesor_dict, sep='_')
        expanded.columns = [f"asesor_{col}" for col in expanded.columns]  # Agregar prefijo 'asesor_' a todas las columnas anidadas
        return expanded.iloc[0]
    except Exception as e:
        print(f"Error al procesar el asesor: {asesor} - {e}")
        return pd.Series({'error': 'Error de procesamiento'})



# Aplicar la función para expandir la columna 'asesor'
df_expanded = df['asesor'].apply(expand_asesor)

# Concatenar con el DataFrame original y agregar las columnas expandidas al final
df_final = pd.concat([df.drop('asesor', axis=1), df_expanded], axis=1)

# Guardar el archivo resultante
output_path = "/dbfs/mnt/PagWeb/Extract/Separados/separados_web_asesorExpanded.csv"
df_final.to_csv(output_path, index=False)
print(f"Datos expandidos guardados en: {output_path}")


# Mostrar los resultados
display(df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ##borrar columna error

# COMMAND ----------

import pandas as pd

#leer el archivo csv original
df = pd.read_csv("/dbfs/mnt/PagWeb/Extract/Separados/separados_web_asesorExpanded.csv")

df = df.drop('error', axis=1)

#guardar el archivo modificado con nuevo nombre
df.to_csv("/dbfs/mnt/PagWeb/Extract/Separados/separados_web_SinError.csv", index=False)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##limpiar columna fecha quitando la zona horaria

# COMMAND ----------

import pandas as pd

# Leer el archivo CSV
input_path = "/dbfs/mnt/PagWeb/Extract/Separados/separados_web_SinError.csv"
df = pd.read_csv(input_path)

# Modificar el contenido de la columna 'createdAt' para eliminar todo después de la 'T'
df['createdAt'] = df['createdAt'].str.split('T').str[0]

# Guardar el archivo actualizado
output_path = "/dbfs/mnt/PagWeb/Extract/Separados/separados_web_CreatedClean.csv"
df.to_csv(output_path, index=False)

print(f"Archivo modificado guardado en: {output_path}")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##convertir columna a datatime y guardarlo, solo tomando fecha

# COMMAND ----------

import pandas as pd

# Leer el archivo CSV
df = pd.read_csv("/dbfs/mnt/PagWeb/Extract/Separados/separados_web_CreatedClean.csv")

# Convertir la columna 'createdAt' a datetime y extraer solo la fecha
df['createdAt'] = pd.to_datetime(df['createdAt']).dt.date

# Guardar el DataFrame actualizado en un nuevo archivo CSV
df.to_csv("/dbfs/mnt/PagWeb/Extract/Separados/separados_web_date.csv", index=False)

# Mostrar el DataFrame resultante
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ##instalar libreria para convertir numeros romanos a naturales
# MAGIC

# COMMAND ----------

pip install roman

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
import roman

# Leer el archivo CSV
df = pd.read_csv("/dbfs/mnt/PagWeb/Extract/Separados/separados_web_date.csv")

# Función para convertir números romanos a enteros
def convert_roman_to_int(roman_value):
    try:
        return roman.fromRoman(roman_value)
    except roman.InvalidRomanNumeralError:
        return None  # Manejar errores en caso de valores no válidos

# Aplicar la conversión a la columna
df['lote_manzana_etapa_nombre'] = df['lote_manzana_etapa_nombre'].apply(convert_roman_to_int)

# Guardar el archivo actualizado
df.to_csv("/dbfs/mnt/PagWeb/Extract/Separados/separados_web_romanosConvertidos.csv", index=False)
print("Conversión completada y archivo guardado.")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##convertir columna a datatime y guardarlo, solo tomando la fecha

# COMMAND ----------

import pandas as pd

#leer el archivo CSV
df = pd.read_csv("/dbfs/mnt/PagWeb/Extract/Separados/separados_web_romanosConvertidos.csv")

#convertir la columna 'createdAd' a datetime y extraer solo la fecha
df['createdAt']= pd.to_datetime(df['createdAt']).dt.date

#Guardar el dataframe actualizado en un nuevo archivo
df.to_csv("/dbfs/mnt/PagWeb/Extract/Separados/separados_web_dateConvert.csv", index=False)

#mostrar 
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## cambiar espacios por guiones bajos

# COMMAND ----------

import pandas as pd

# Leer el archivo CSV
df = pd.read_csv("/dbfs/mnt/PagWeb/Extract/Separados/separados_web_dateConvert.csv")

# Lista de columnas a modificar
columnas_a_modificar = ['asesor_nombre', 'cliente_nombreCompleto', 'lote_manzana_etapa_fraccionamiento_nombre'] #se agregan las columnas necesarias

# Reemplazar espacios por guiones bajos en las columnas seleccionadas
for columna in columnas_a_modificar:
    df[columna] = df[columna].apply(lambda x: x.replace(' ', '_') if isinstance(x, str) else x)

# Guardar el DataFrame modificado en un nuevo archivo CSV
df.to_csv("/dbfs/mnt/PagWeb/Extract/Separados/separados_web_guionesBajos.csv", index=False)

# Mostrar el DataFrame modificado
display(df)




# COMMAND ----------

# MAGIC %md
# MAGIC ## Eliminar los guiones bajos al final de las palabras(por todos los espacios que se generaron en blanco)

# COMMAND ----------

import pandas as pd

df = pd.read_csv("/dbfs/mnt/PagWeb/Extract/Separados/separados_web_guionesBajos.csv")

# Eliminar los guiones bajos al final de las palabras en la columna 'asesor_nombre'
df['asesor_nombre'] = df['asesor_nombre'].str.rstrip('_')
df['cliente_nombreCompleto'] = df['cliente_nombreCompleto'].str.rstrip('_')
df['lote_manzana_etapa_fraccionamiento_nombre'] = df['lote_manzana_etapa_fraccionamiento_nombre'].str.rstrip('_')

# Guardar el DataFrame actualizado en un nuevo archivo CSV
df.to_csv("/dbfs/mnt/PagWeb/Extract/Separados/separados_web_AllClean.csv", index=False)

# Mostrar el DataFrame actualizado
display(df)

