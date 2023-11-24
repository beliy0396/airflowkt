from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from datetime import datetime, timedelta

default_args = {
    'owner': 'Belomestnov.MaS',
    'email': ['BelomestnovVA@vvsu.ithub.ru'],
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'depends_on_past': False,
    'email_on_failure': False,
    'priority_weight': 10,
    'execution_timeout': timedelta(hours=10),
    'dag_run_timeout': timedelta(hours=10)
}

"""
Описание DAG
"""
with DAG(
        dag_id='first_dag',
        schedule_interval='0 19 * * *',
        start_date=datetime(2022, 1, 1),
        catchup=False,
        description=f"""First dag""",
        tags=['firs_dag'],
        default_args=default_args
) as dag:
    
    first_task = BashOperator(task_id='firs_task', bash_command=f"python3 /opt/airflow/dags/dag_test.py")
    second_task = BashOperator(task_id='second_task', bash_command=f"python3 /opt/airflow/dags/dag_test.py")

    @task()
    def func():
        print("Hello world")

    first_task >> second_task >> func()
