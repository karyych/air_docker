import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from sklearn.preprocessing import StandardScaler
import pandas as pd


def st_iris_data():
    input_file = "/path/inside/container/WineQT.csv"
    data = pd.read_csv(input_file)
    st = StandardScaler()
    columns_standart = ['fixed acidity', 'volatile acidity']
    data[columns_standart] = st.fit_transform(data[columns_standart])
    out_file = "/path/inside/container/WineQT_boss.csv"
    data.to_csv(out_file, index=False)

dag = DAG(
    dag_id='iris_dags',
    start_date=datetime(2023, 10, 8),
    schedule_interval=None,
    catchup=False
)
st_iris_data_task = PythonOperator(
    task_id='st_iris_data_task',
    python_callable=st_iris_data,
    dag=dag,
)

st_iris_data_task
