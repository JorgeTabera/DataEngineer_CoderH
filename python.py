import requests
import json
import psycopg2
from psycopg2 import sql

# Configuración de la conexión a Redshift
REDSHIFT_HOST = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
REDSHIFT_PORT = '5439'
REDSHIFT_DB = 'data-engineer-database'
REDSHIFT_USER = 'taberajorge_coderhouse'
REDSHIFT_PASSWORD = 'O956VwKRBB'

def extract_data():
    """
    Función para extraer datos de la API de CoinGecko.
    """
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        'vs_currency': 'usd',
        'ids': 'bitcoin,ethereum,ripple',
        'order': 'market_cap_desc',
        'per_page': 3,
        'page': 1
    }
    
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error en la solicitud: {response.status_code}")
        return None

def transform_data(data):
    """
    Función para transformar los datos extraídos en un diccionario organizado.
    """
    transformed_data = []
    for coin in data:
        coin_info = {
            'id': coin['id'],
            'name': coin['name'],
            'symbol': coin['symbol'],
            'current_price': coin['current_price'],
            'market_cap': coin['market_cap'],
            'total_volume': coin['total_volume']
        }
        transformed_data.append(coin_info)
    return transformed_data

def load_data_to_redshift(data):
    """
    Función para cargar los datos transformados en Amazon Redshift.
    """
    try:
        conn = psycopg2.connect(
            dbname=REDSHIFT_DB,
            user=REDSHIFT_USER,
            password=REDSHIFT_PASSWORD,
            host=REDSHIFT_HOST,
            port=REDSHIFT_PORT
        )
        cursor = conn.cursor()

        insert_query = sql.SQL("""
            INSERT INTO cryptocurrencies (id, name, symbol, current_price, market_cap, total_volume)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE
            SET name = EXCLUDED.name,
                symbol = EXCLUDED.symbol,
                current_price = EXCLUDED.current_price,
                market_cap = EXCLUDED.market_cap,
                total_volume = EXCLUDED.total_volume;
        """)

        for coin in data:
            cursor.execute(insert_query, (
                coin['id'],
                coin['name'],
                coin['symbol'],
                coin['current_price'],
                coin['market_cap'],
                coin['total_volume']
            ))

        conn.commit()
        cursor.close()
        conn.close()
        print("Datos cargados en Redshift correctamente.")
    
    except Exception as e:
        print(f"Error al cargar los datos en Redshift: {e}")

def main():
    """
    Función principal que coordina el proceso de ETL.
    """
    print("Extrayendo datos...")
    raw_data = extract_data()
    
    if raw_data:
        print("Transformando datos...")
        transformed_data = transform_data(raw_data)
        
        print("Cargando datos en Redshift...")
        load_data_to_redshift(transformed_data)

if __name__ == "__main__":
    main()
