Proyecto de Data Engineering: Extracción de Datos de Spotify

Este proyecto realiza la extracción de los 50 tracks de la playlist "Top 50 Argentina" de la API de Spotify y los carga en una base de datos Redshift. El proceso se implementa como un ETL (Extracción, Transformación y Carga) utilizando Python y se gestiona con Airflow.

Estructura del Proyecto:

ETLSpotify.py: Script principal que realiza la extracción y carga de datos.
run_etl.py: Script para ejecutar el proceso ETL manualmente.
Dags_PruebaMailSpotify.py: Archivo de configuración del DAG en Airflow para programar la ejecución automática del ETL.
Credentials.json: Archivo que contiene las credenciales necesarias para acceder a la API de Spotify y Redshift.
Requisitos:

Python 3 o superior
Bibliotecas: psycopg2, sqlalchemy, requests, pandas, apache-airflow
Credenciales válidas de Spotify y Redshift.
Instalación:

Clona el repositorio.
Instala las dependencias requeridas con pip.
Configura tus credenciales en Credentials.json.
Uso: Para ejecutar el proceso ETL manualmente, usa el comando: python run_etl.py. Para programar la ejecución automática, configura Airflow y asegúrate de que el DAG está habilitado.

Notificaciones: Recibirás un correo electrónico notificando el estado del proceso ETL (completado o fallido).
