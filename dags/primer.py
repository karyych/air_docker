from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

dag = DAG(dag_id='my_dag', start_date=datetime.now())

task1 = BashOperator(
    task_id='task1',
    bash_command='echo "Hello from task1"',
    dag=dag,
)

task2 = BashOperator(
    task_id='task2',
    bash_command='echo "Hello from task2"',
    dag=dag,
)

task3 = BashOperator(
    task_id='task3',
    bash_command='echo "Hello from task3"',
    dag=dag,
)

