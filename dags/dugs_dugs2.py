import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import DagBag
from datetime import datetime, timedelta

# Путь к каталогу, в котором находятся ваши даг-файлы
dag_directory = '/path/inside/container/'

# Функция для автоматической регистрации дагов
def register_dags():
    dagbag = DagBag(dag_directory)

    for dag_id, dag in dagbag.dags.items():
        dag.sync_to_db()

# Регулярное выполнение функции для регистрации новых дагов
register_dags()

# Создайте DAG, который будет выполнять функцию регистрации
register_dags_dag = DAG(
    'register_dags',
    default_args={
        'owner': 'your_name',
        'start_date': datetime(2023, 1, 1),
    },
    schedule_interval=timedelta(minutes=2),  # Установите частоту проверки каталога, например, каждый час
)
# Создайте задачу, которая будет выполнять функцию регистрации
register_dags_task = PythonOperator(
    task_id='register_dags_task',
    python_callable=register_dags,
    dag=register_dags_dag,
)

register_dags_task  # Не забудьте указать зависимость задачи

if __name__ == "__main__":
    register_dags_dag.clear()
    register_dags_dag.run()
