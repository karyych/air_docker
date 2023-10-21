from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException

# Определение аргументов DAG
default_args = {
    'owner': 'your_name',
    'start_date': datetime(2023, 10, 19),
    'retries': 1,
}

# Создание объекта DAG
dag = DAG(
    dag_id='post'
    'postgres_connection_check',
    default_args=default_args,
    schedule_interval=None,  # Убедитесь, что это расписание отключено
    catchup=False,  # Отключение выполнения задач для предыдущих дат
    is_paused_upon_creation=False,
    max_active_runs=1,
)

# Функция для проверки подключения
def check_postgres_connection():
    try:
        # Создайте объект PostgresHook с идентификатором соединения (Conn Id)
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn', host='f5aed4bfa13306dffcf6b96f3b5b613c4700906c64e10c26442708fccf552098', port=5432)

        # Выполните запрос, который не изменяет состояние базы данных (например, SELECT 1)
        result = pg_hook.get_first("SELECT 1")

        # Проверьте, что запрос вернул корректный результат
        if result[0] == 1:
            print("Подключение к PostgreSQL успешно.")
        else:
            print("Подключение к PostgreSQL не успешно.")
    except AirflowException as e:
        print(f"Ошибка при проверке подключения: {str(e)}")

# Создание задачи для выполнения проверки подключения
check_postgres_connection_task = PythonOperator(
    task_id='check_postgres_connection',
    python_callable=check_postgres_connection,
    dag=dag
)
