# Databricks notebook source
# Contenido del archivo JSON de credenciales
credentials_json = """
{
  "type": "service_account",
  "project_id": "pecsadatascience",
  "private_key_id": "9ac3c6e20a0ca5148af5a8946c9d2070c367acad",
  "private_key": "-----BEGIN PRIVATE KEY-----\\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCaHLMddIRW/WT2\\nof7+VYqIMolHedArHPQwH/GGwUb73EmVTSNwCSc69sxwTK07N8dUu4comCgEyRK3\\nlGy3vqKo3gBQhW3IR0wAu5csqKtyFm31kFZGyZGHWwykhlkR7nHoWHBVX01pOhgw\\n6wkpEcHvHsKVgRqITEvurvuJ/pixVC1f9huHhe/mo/cwN/WdFPSZRozu6frvi7sP\\n5AlmDQixr3Vhv3ZoBFWQ9gTUGBOhqrC1+9O2x8lZ48yq79SrMJoRTQeGjXf4Pozi\\nfHAJ6e6Tt9yJyVZUdSmVdY2zMkXwpPtbzO2kUBlqwjIUr4w+w8JRDJgYgbhHCteB\\nFrGP7+aVAgMBAAECggEAFA+WVUg8jeJQSTaPLeZOsSn66r31lhcfxj/yPbtdFR34\\nyiGPcl8Op0NHG3svtZyuKt56WpNr6iuOby039bcwD1FOsyybPM3jG9lPdXXDU4eN\\n6sBOKsrEUd1NSQFgCNCELV2Nyd8tAXyZd4yw1ZLFLu5PIAz9Oi7MV5aZkb39fF+o\\nBXhKKUZd7QQadzHnKNcrq36a9zBdC0gurl5neaTr7y0yp+xxutqpmc8yU4Ggu0Qh\\nbyYQ1Xh4IpxnM6LEYEdjs1EEfunYclK0kDKEDpinzogsVPjokC59C/WamhYkQqcV\\nr0Y87D64HV976wycvTk4QtViIAusuhrwYgbi2mCmwQKBgQDN/cjoW9kbkSviR3di\\nMwce1IPMcxzddOfNKfu7WkR5riX0kZ+nApUBvtjdpp08XIhjabRtomRbf/8BBvFe\\nulnTJ1nlO6ejkWu7JoCZs6rxbnvy768syFNOIRsWrWQwfjArxwAErP80SIa+Exuv\\nlSDUS4R4lpnybZ6AzNgQ9gyakQKBgQC/hqg+neaHeKr2DULcXzYPi2B2EcW55Hj1\\naeGHFxWpkXEkO+klRVB3IFJDdEzzgs4T8aOdTwPswOF4Xd2hoZ/VJYZFEvt0bivE\\nWoPETHEmhWky8AnaDY5cLR/XkYA7aeTC6DIPz+0MBYbD14zYrS27sj1YB7AdTME3\\nndm2noYlxQKBgQCTx9Nf56ztWwvZrZ3XZHZiRuI7RgZaVmmKRelkMtFXXnm6aNLZ\\n9T9DXlClS8gYKuZqM8aOtOc8waoHaZy2cgjJNL0IePC+pzBbtraiDkTAKpxf4FaT\\nUTB6p8OQVFrBc3ZJod3AfQAl2TZZnMnLALjptTfXb2wIgV4qV0tXd735EQKBgHQ1\\nq14bBuaKJAmZsEjwiGfUJNpfHD8XzwDxlsMWEbHKqwDWTTetIA64ENIW/h3+7zWQ\\nt7raV8JOokAbjD/nRojY9OhyhRp3Qp0oYq03yKwb5OHgXcnt0cRCKQDuqwTh0skk\\nhEOt4OIbdFLeNfq+0XICy0/fkk+k49PwNkqlUzYVAoGBAMrMahUp5o7M9KD1Dw/4\\nPVI5fmcFMTMcTV8qSFoHGxGI4odoEYYZooFoDtvH9MBksQ1OGyt0/RNWvj9SSPKC\\nspgExGOPNtSi7okQUVmjkywNWqFmk/YEYMNp1wLSDZdf7JzXl1AzSeZ1Ux0gaev/\\nWlpg62uLfARKzJdCHk1cLxS8\\n-----END PRIVATE KEY-----\\n",
  "client_email": "cuenta-googlesheets-pecsa@pecsadatascience.iam.gserviceaccount.com",
  "client_id": "116400018560091003127",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/cuenta-googlesheets-pecsa%40pecsadatascience.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}
"""

# Ruta en DBFS donde deseas guardar el archivo
path = "/mnt/GoogleSheets/Credenciales/GoogleSheets_Credenciales.json"

# Guarda el archivo JSON en DBFS
dbutils.fs.put(path, credentials_json, overwrite=True)

print(f"Archivo subido a {path}")


# COMMAND ----------

# Verificar si el archivo existe
file_path = "/dbfs/mnt/GoogleSheets/Credenciales/GoogleSheets_Credenciales.json"

try:
    with open(file_path, "r") as file:
        print("El archivo JSON existe y está disponible.")
except FileNotFoundError:
    print("El archivo JSON no existe. Revisa la ruta o asegúrate de haber subido el archivo.")

