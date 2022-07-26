from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task, task_group
from spark_plugin.operators.spark import SparkSubmitWithCredentialsOperator
from spark_plugin.utils.lookup import ConfFromConnection as SparkConfFromConnection
from cgu_servidores.operators.file_storage import MINIO_BUCKET
from cgu_servidores.operators.database import INDEX_KEY
from cgu_servidores.operators.aggregation import (
    REGISTER_KEY,
    SALARY_KEY,
    REMOVED_FROM_POSITION_KEY,
    OBSERVATION_KEY,
    HONORARY_ADVOCATIVE_KEY,
    HONORARY_JETONS_KEY,
)


@task_group
def spark(indices: List[Dict[str, str]], filetype: str) -> TaskGroup:
    @task(task_id=f'format_{filetype}_spark_conf', multiple_outputs=False)
    def format_spark_conf(indices: Dict[str, str]) -> Dict[str, Any]:
        register_bucket = f'{MINIO_BUCKET}/{indices[REGISTER_KEY]}'
        salary_bucket = f'{MINIO_BUCKET}/{indices[SALARY_KEY]}'
        observation_bucket = f'{MINIO_BUCKET}/{indices[OBSERVATION_KEY]}'

        removal_bucket = (
            f'{MINIO_BUCKET}/{indices[REMOVED_FROM_POSITION_KEY]}'
            if indices[REMOVED_FROM_POSITION_KEY]
            else ''
        )
        honorary_advocative_bucket = (
            f'{MINIO_BUCKET}/{indices[HONORARY_ADVOCATIVE_KEY]}'
            if indices[HONORARY_ADVOCATIVE_KEY]
            else ''
        )
        honorary_jetons_bucket = (
            f'{MINIO_BUCKET}/{indices[HONORARY_JETONS_KEY]}'
            if indices[HONORARY_JETONS_KEY]
            else ''
        )

        return {
            'spark.aletheia.buckets.register': register_bucket,
            'spark.aletheia.buckets.salary': salary_bucket,
            'spark.aletheia.buckets.observation': observation_bucket,
            'spark.aletheia.buckets.removal': removal_bucket,
            'spark.aletheia.buckets.honorary.advocative': honorary_advocative_bucket,
            'spark.aletheia.buckets.honorary.jetons': honorary_jetons_bucket,

            'spark.es.resource': indices[INDEX_KEY],
        }

    SparkSubmitWithCredentialsOperator.partial(
        retries=2,
        retry_delay=timedelta(seconds=300),
        max_active_tis_per_dag=3,
        do_xcom_push=False,
        task_id=f'{filetype}_spark_job',
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
            'com.amazonaws:aws-java-sdk-pom:1.12.164',
            'org.apache.hadoop:hadoop-aws:3.3.1',
            'org.elasticsearch:elasticsearch-spark-30_2.12:8.3.2',
        ],
    ).expand(conf=format_spark_conf.expand(indices=indices))
