FROM apache/airflow:2.7.2
EXPOSE 8080
RUN pip install apache-airflow
COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt