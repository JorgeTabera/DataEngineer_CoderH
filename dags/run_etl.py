import sys
import os

# Agregar la ruta al directorio donde está ETLSpotify.py
sys.path.append(r"C:\Vaio\Academias\CoderHouse\DataEngineering\PF_Spotify\dags")

from ETLSpotify import execute_etl  # Importar solo la función necesaria

def main():
    # Ejecutar el proceso ETL para PlaylistTracks
    execute_etl()
    
    print("ETL Generado OK")

if __name__ == "__main__":
    main()
