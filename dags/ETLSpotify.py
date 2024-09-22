import pandas as pd
import requests
import json
import os
import base64
import psycopg2

# Cargar las credenciales desde el archivo Credentials.json
def load_credentials():
    credentials_path = os.path.join(os.path.dirname(__file__), "Credentials.json")  # Ruta absoluta
    with open(credentials_path) as f:
        return json.load(f)

# Obtener el token de acceso de Spotify
def get_token(credentials):
    ClientID = credentials["ClientID"]
    ClientSecret = credentials["ClientSecret"]
    auth_str = f"{ClientID}:{ClientSecret}"
    auth_bytes = auth_str.encode("utf-8")
    auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")

    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": "Basic " + auth_base64,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {"grant_type": "client_credentials"}
    
    result = requests.post(url, headers=headers, data=data)
    json_result = json.loads(result.content)
    return json_result["access_token"]

# Obtener datos de la playlist Top 50 Argentina
def Top50(token, playlist_id):
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    result = requests.get(url, headers=headers)
    if result.status_code != 200:
        print("Error:", result.status_code, result.content)
        return None
    
    return json.loads(result.content)

# Conexión a Redshift
def connect_redshift(credentials):
    conn = psycopg2.connect(
        host=credentials["REDSHIFT_HOST"],
        port=credentials["REDSHIFT_PORT"],
        dbname=credentials["REDSHIFT_DB"],
        user=credentials["REDSHIFT_USER"],
        password=credentials["REDSHIFT_PASSWORD"]
    )
    return conn

# Insertar datos de la playlist en la base de datos
def insert_playlist_data(playlist_data, cursor, conn):
    collaborative = playlist_data.get("collaborative")
    description = playlist_data.get("description")
    external_urls = playlist_data["external_urls"].get("spotify")
    followers = playlist_data["followers"]["total"]
    href = playlist_data.get("href")
    playlist_id = playlist_data.get("id")
    name = playlist_data.get("name")
    owner = playlist_data["owner"].get("display_name")
    public = playlist_data.get("public")
    snapshot_id = playlist_data.get("snapshot_id")
    playlist_type = playlist_data.get("type")
    uri = playlist_data.get("uri")
    
    query = """
    INSERT INTO PlaylistTop50ARG (collaborative, description, external_urls, followers, href, id, name, owner, public, snapshot_id, tracks, type, uri)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(query, (collaborative, description, external_urls, followers, href, playlist_id, name, owner, public, snapshot_id, None, playlist_type, uri))
    conn.commit()

    # Insertar los tracks
    tracks = playlist_data['tracks']['items']
    for item in tracks:
        track = item['track']
        track_name = track['name']
        track_id = track['id']
        track_href = track['href']
        track_uri = track['uri']
        
        track_query = """
        INSERT INTO PlaylistTracks (playlist_id, track_id, track_name, track_href, track_uri, fechaingreso)
        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        """
        cursor.execute(track_query, (playlist_id, track_id, track_name, track_href, track_uri))
        conn.commit()

# Función principal de ETL
def execute_etl():
    credentials = load_credentials()
    token = get_token(credentials)
    playlist_id = "37i9dQZEVXbMMy2roB9myp"
    playlist_data = Top50(token, playlist_id)

    if playlist_data:
        conn = connect_redshift(credentials)
        cursor = conn.cursor()

        # Limpiar la tabla antes de insertar nuevos datos
        cursor.execute("TRUNCATE TABLE PlaylistTracks;")
        
        insert_playlist_data(playlist_data, cursor, conn)
        
        cursor.close()
        conn.close()
        
        print("Datos ingresados correctamente a la tabla PlaylistTracks")

# Ejecutar el proceso ETL
if __name__ == "__main__":
    execute_etl()
