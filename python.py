import requests
import pandas as pd
import json
from sqlalchemy import create_engine, text

# Paso 1: Extraer datos desde la API de Weatherbit.io
api_key = 'cfe5adde1a2b4abd8e953a3cd14f8f12'
ciudad = 'Argentina'
url = f'http://api.weatherbit.io/v2.0/current?city={ciudad}&key={api_key}'

try:
    response = requests.get(url)
    response.raise_for_status()  # Lanza una excepción si la respuesta tiene un código de error
    data = response.json()
except requests.RequestException as e:
    print(f"Error en la solicitud a la API: {e}")
    data = {}
except json.JSONDecodeError as e:
    print(f"Error al decodificar la respuesta JSON: {e}")
    data = {}

if 'data' not in data or not data['data']:
    print("No se encontraron datos en la respuesta de la API.")
else:
    # Paso 2: Transformar los datos
    weather_data = data['data'][0]  # Extrae el primer (y único) registro

    registro = {
        'ciudad': weather_data.get('city_name', 'Desconocida'),
        'fecha': weather_data.get('ob_time', '').split(' ')[0],
        'temperatura': weather_data.get('temp', None),
        'presion': weather_data.get('pres', None),
        'humedad': weather_data.get('rh', None),
        'velocidad_viento': weather_data.get('wind_spd', None),
        'descripcion_clima': weather_data.get('weather', {}).get('description', 'Desconocida')
    }

    # Convertir el registro a un DataFrame
    df = pd.DataFrame([registro])

    # Paso 3: Cargar los datos en Amazon Redshift

    # Cargar credenciales desde el archivo Config.json
    try:
        with open('Config.json', 'r') as file:
            credentials = json.load(file)
    except FileNotFoundError:
        print("El archivo Config.json no se encontró.")
        credentials = {}
    except json.JSONDecodeError:
        print("Error al decodificar el archivo Config.json.")
        credentials = {}

    # Asignar variables desde el JSON
    REDSHIFT_HOST = credentials.get('REDSHIFT_HOST')
    REDSHIFT_PORT = credentials.get('REDSHIFT_PORT')
    REDSHIFT_DB = credentials.get('REDSHIFT_DB')
    REDSHIFT_USER = credentials.get('REDSHIFT_USER')
    REDSHIFT_PASSWORD = credentials.get('REDSHIFT_PASSWORD')

    if not all([REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DB, REDSHIFT_USER, REDSHIFT_PASSWORD]):
        print("Faltan credenciales en el archivo Config.json.")
    else:
        try:
            # Conectar a Redshift
            engine = create_engine(f'redshift+psycopg2://{REDSHIFT_USER}:{REDSHIFT_PASSWORD}@{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DB}')

            # Probar conexión
            with engine.connect() as connection:
                result = connection.execute("SELECT 1")
                print("Conexión a Redshift exitosa:", result.fetchone())

            # Crear sentencia de inserción
            with engine.connect() as connection:
                for index, row in df.iterrows():
                    insert_query = text("""
                        INSERT INTO datos_meteorologicos (ciudad, fecha, temperatura, presion, humedad, velocidad_viento, descripcion_clima)
                        VALUES (:ciudad, :fecha, :temperatura, :presion, :humedad, :velocidad_viento, :descripcion_clima)
                    """)
                    connection.execute(insert_query, **row.to_dict())

            print("Datos insertados exitosamente en Redshift.")
        except Exception as e:
            print(f"Error al conectar a Redshift o cargar datos: {e}")
