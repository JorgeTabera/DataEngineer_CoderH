import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Configuración de la conexión a PostgreSQL
DATABASE_URI = 'postgresql+psycopg2://airflow:airflow@localhost:5432/postgres'

def create_database_and_table():
    try:
        # Conectar a la base de datos por defecto
        conn = psycopg2.connect("dbname=postgres user=airflow password=airflow host=localhost port=5432")
        conn.autocommit = True  # Habilitar autocommit para evitar problemas con transacciones
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_database WHERE datname='spotify_db'")
            exists = cursor.fetchone() is not None

            if not exists:
                cursor.execute("CREATE DATABASE spotify_db")
                print("Base de datos spotify_db creada exitosamente.")
            else:
                print("La base de datos spotify_db ya existe.")

        # Cerrar la conexión a la base de datos por defecto
        conn.close()

        # Cambiar la URI para conectarse a spotify_db
        spotify_db_engine = create_engine('postgresql+psycopg2://airflow:airflow@localhost:5432/spotify_db')

        # Crear la tabla PlaylistTracks en la base de datos spotify_db
        with spotify_db_engine.connect() as connection:
            connection.execute(text("""
            CREATE TABLE IF NOT EXISTS PlaylistTracks (
                Playlist_id text,
                track_id TEXT PRIMARY KEY,
                track_name TEXT,
                track_href TEXT,
                track_uri TEXT,
                fechaingreso TIMESTAMP DEFAULT CURRENT_TIMESTAMP  
            )
            """))   ## Codigo corregido con la columna Fechaingreso
            print("Tabla PlaylistTracks creada exitosamente.")

    except SQLAlchemyError as e:
        print(f"Error al crear la base de datos o la tabla: {e}")
    except Exception as e:
        print(f"Error inesperado: {e}")

if __name__ == "__main__":
    create_database_and_table()
