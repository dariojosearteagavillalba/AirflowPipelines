from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def print_hello():
    print("Hola")

def print_world():
    print("Mundo")

# Define los argumentos del DAG
default_args = {
    'owner': 'tú',
    'start_date': datetime(2023, 8, 29),
    'retries': 1,
}

# Crea el objeto DAG
dag = DAG(
    'ejemplo_basico',
    default_args=default_args,
    schedule_interval=None,  # Ejecución manual, puedes configurar un horario aquí
    catchup=False,  # No ponerse al día con ejecuciones antiguas
)

# Define las tareas usando PythonOperator
task_print_hello = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

task_print_world = PythonOperator(
    task_id='print_world',
    python_callable=print_world,
    dag=dag,
)

# Configura las dependencias de las tareas
task_print_hello >> task_print_world
