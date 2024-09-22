from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
import subprocess
import os

# Funciones de callback para el envío de correos
def enviar_email_proceso_completado(context):
    email = EmailOperator(
        task_id='enviar_email_completado',
        to='jorgetaberacoderhouse@gmail.com',
        subject='ETL de Spotify Ejecutado - Proceso Completado',
        html_content="""<h3>El proceso ETL se ha ejecutado correctamente.</h3>""",
    )
    email.execute(context)

def enviar_email_proceso_fallado(context):
    email = EmailOperator(
        task_id='enviar_email_fallado',
        to='jorgetaberacoderhouse@gmail.com',
        subject='ETL de Spotify Ejecutado - Proceso Fallado',
        html_content="""<h3>El proceso ETL ha fallado.</h3>""",
    )
    email.execute(context)

# Función para ejecutar el script ETL
def ejecutar_script_etl():
    script_path = os.path.join(os.path.dirname(__file__), "run_etl.py")  # Usando ruta absoluta

    result = subprocess.run(['python', script_path], capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"El script falló con el siguiente error: {result.stderr}")

# Configuración del DAG
default_args = {
    'owner': 'Jorge_Tabera',
    'start_date': datetime(2024, 9, 22, 19, 40),
    'retries': 1,
    'email_on_failure': False,
    'email_on_retry': True,
    'email': ['jorgetaberacoderhouse@gmail.com']
}

dag = DAG(
    dag_id='Dags_PruebaMailSpotify',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

# Definición de las tareas
t1 = PythonOperator(
    task_id='ejecutar_etl',
    python_callable=ejecutar_script_etl,
    on_success_callback=enviar_email_proceso_completado,
    on_failure_callback=enviar_email_proceso_fallado,
    dag=dag
)

t1
