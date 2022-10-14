from datetime import timedelta
from pathlib import Path
from typing import Dict, List
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task, task_group
from spark_plugin.operators.spark import SparkSubmitWithCredentialsOperator
from spark_plugin.utils.lookup import ConfFromConnection as SparkConfFromConnection
from cgu_bolsafamilia.operators.file_storage import MINIO_BUCKET
from cgu_bolsafamilia.operators.database import PEOPLE_INDEX_KEY
from cgu_bolsafamilia.operators.file_storage import PAYMENT_KEY, WITHDRAW_KEY


@task_group
def spark(indices: List[Dict[str, str]]) -> TaskGroup:
    @task(multiple_outputs=False)
    def format_spark_conf(indices: Dict[str, str]) -> str:
        for key in [PAYMENT_KEY, WITHDRAW_KEY]:
            path = indices[key][0]

            if path[0] == '/':
                path = path[1:]

            indices[key] = f'{MINIO_BUCKET}/{path}'

        return {
            'spark.aletheia.buckets.payment': indices[PAYMENT_KEY],
            'spark.aletheia.buckets.withdraw': indices[WITHDRAW_KEY],

            'spark.es.resource': indices[PEOPLE_INDEX_KEY],
        }

    SparkSubmitWithCredentialsOperator.partial(
        retries=2,
        retry_delay=timedelta(seconds=300),
        max_active_tis_per_dag=3,
        do_xcom_push=False,
        task_id=PEOPLE_INDEX_KEY,
        application=(Path(__file__).parent.parent / 'spark' / f'{PEOPLE_INDEX_KEY}.py').as_posix(),
        verbose=False,
        conn_id='spark_default',
        executor_memory='2G',
        total_executor_cores=3,
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
    ).expand(conf=format_spark_conf.expand(indices=indices))
