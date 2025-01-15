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

# URL base de la API
BASE_URL = "https://api.casaspecsa.com/api/venta/all-transacciones?estatus=Cancelado"

# Parámetros iniciales
limit = 10  # Número de registros por página
page = 1  # Página inicial
all_data = []  # Lista para almacenar todos los registros

# Ruta de salida en Databricks
output_dir = "/dbfs/mnt/PagWeb/Extract"
output_file = os.path.join(output_dir, "cancelados_web.csv")

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

