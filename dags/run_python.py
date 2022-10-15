from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

with DAG(
    "my_dag_name", 
    start_date=datetime(2022, 10, 10),
    schedule_interval="@daily"
) as dag:
    bash_task = BashOperator(
    task_id='run_main_file',
    bash_command='python /opt/airflow/main.py'
    # 
)
