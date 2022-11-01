FROM python:3.8-slim

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y openjdk-11-jdk libmagic1 libpq-dev build-essential

RUN pip install -U pip wheel

WORKDIR /opt/airflow

COPY requirements.txt .

RUN pip install -Ur requirements.txt
RUN pip install -U apache-airflow[celery,postgres,elasticsearch,redis,apache.spark]==2.4.2 \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.4.2/constraints-3.8.txt"

COPY . .

# RUN chmod +x entrypoint.sh
# ENTRYPOINT [ "./entrypoint.sh" ]
