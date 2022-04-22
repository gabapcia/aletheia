FROM python:3.8-slim

WORKDIR /opt/airflow

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y openjdk-11-jdk libmagic1

RUN pip install -U pip wheel

COPY . .
RUN pip install -Ur requirements.txt
RUN pip install -U apache-airflow[celery,postgres,redis,apache.spark,apache.cassandra]==2.2.5 \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.2.5/constraints-3.8.txt"

# RUN chmod +x entrypoint.sh
# ENTRYPOINT [ "./entrypoint.sh" ]
