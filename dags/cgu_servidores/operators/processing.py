from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task, task_group
from spark_plugin.operators.spark import SparkSubmitWithCredentialsOperator
from spark_plugin.utils.lookup import ConfFromConnection as SparkConfFromConnection
from cgu_servidores.operators.database import INDEX_KEY
from cgu_servidores.operators.file_storage import (
    MINIO_BUCKET,
    EXTRACTED_FILES_KEY,

    REGISTER_KEY,
    SALARY_KEY,
    REMOVED_FROM_POSITION_KEY,
    OBSERVATION_KEY,

    HONORARY_KEY,
    HONORARY_ADVOCATIVE_KEY,
    HONORARY_JETONS_KEY,
)


def spark(catalog: List[Dict[str, str]], filetype: str) -> TaskGroup:
    with TaskGroup(group_id=f'{filetype}_spark', prefix_group_id=False) as spark:
        @task(task_id=f'{filetype}_conf', multiple_outputs=False)
        def format_spark_conf(catalog: List[Dict[str, str]]) -> List[Dict[str, str]]:
            confs = list()

            for c in catalog:
                register = f"{MINIO_BUCKET}{c[EXTRACTED_FILES_KEY][REGISTER_KEY]}"
                salary = f"{MINIO_BUCKET}{c[EXTRACTED_FILES_KEY][SALARY_KEY]}"
                observation = f"{MINIO_BUCKET}{c[EXTRACTED_FILES_KEY][OBSERVATION_KEY]}"

                removal = (
                    f"{MINIO_BUCKET}{c[EXTRACTED_FILES_KEY][REMOVED_FROM_POSITION_KEY]}"
                    if c[EXTRACTED_FILES_KEY].get(REMOVED_FROM_POSITION_KEY, '')
                    else ''
                )
                honorary_advocative = (
                    f"{MINIO_BUCKET}{c[HONORARY_KEY][HONORARY_ADVOCATIVE_KEY]}"
                    if c[HONORARY_KEY].get(HONORARY_ADVOCATIVE_KEY, '')
                    else ''
                )
                honorary_jetons = (
                    f"{MINIO_BUCKET}{c[HONORARY_KEY][HONORARY_JETONS_KEY]}"
                    if c[HONORARY_KEY].get(HONORARY_JETONS_KEY, '')
                    else ''
                )

                confs.append({
                    'spark.aletheia.buckets.register': register,
                    'spark.aletheia.buckets.salary': salary,
                    'spark.aletheia.buckets.removal': removal,
                    'spark.aletheia.buckets.observation': observation,
                    'spark.aletheia.buckets.honorary.advocative': honorary_advocative,
                    'spark.aletheia.buckets.honorary.jetons': honorary_jetons,

                    'spark.es.resource': c[INDEX_KEY],
                })

            return confs

        SparkSubmitWithCredentialsOperator.partial(
            retries=2,
            retry_delay=timedelta(seconds=300),
            max_active_tis_per_dag=3,
            do_xcom_push=False,
            task_id=f'{filetype}_job',
            application=(Path(__file__).parent.parent / 'spark' / f'{filetype}.py').as_posix(),
            verbose=False,
            conn_id='spark_default',
            executor_memory='1G',
            total_executor_cores=1,
            lazy_conf={
                'spark.hadoop.fs.s3a.path.style.access': 'true',
                'spark.hadoop.fs.s3a.access.key': SparkConfFromConnection(conn_id='minio_default', field='login'),
                'spark.hadoop.fs.s3a.secret.key': SparkConfFromConnection(conn_id='minio_default', field='password'),
                'spark.hadoop.fs.s3a.endpoint': SparkConfFromConnection(
                    conn_id='minio_default',
                    field=['schema', 'host', 'port'],
                    format='{schema}://{host}:{port}',
                ),

                'spark.es.index.auto.create': 'false',
                'spark.es.net.ssl': SparkConfFromConnection(
                    conn_id='elasticsearch_default',
                    field='schema',
                    callback=lambda schema: 'true' if schema == 'https' else 'false',
                ),
                'spark.es.nodes': SparkConfFromConnection(conn_id='elasticsearch_default', field='host'),
                'spark.es.port': SparkConfFromConnection(conn_id='elasticsearch_default', field='port'),
            },
            packages=[
                'com.amazonaws:aws-java-sdk-pom:1.12.319',
                'org.apache.hadoop:hadoop-aws:3.3.1',
                'org.elasticsearch:elasticsearch-spark-30_2.12:8.4.3',
            ],
        ).expand(conf=format_spark_conf(catalog=catalog))

    return spark
