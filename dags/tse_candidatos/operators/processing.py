from datetime import timedelta
from pathlib import Path
from typing import Dict, List
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task, task_group
from spark_plugin.operators.spark import SparkSubmitWithCredentialsOperator
from spark_plugin.utils.lookup import ConfFromConnection as SparkConfFromConnection
from tse_candidatos.operators.file_storage import MINIO_BUCKET
from tse_candidatos.operators.database import INDEX_KEY
from tse_candidatos.operators.scraper import CANDIDATE_KEY, CANDIDATE_ASSETS_KEY, CANCELLATION_REASON_KEY


@task_group
def spark(indices: List[Dict[str, str]]) -> TaskGroup:
    @task(multiple_outputs=False)
    def format_spark_conf(indices: Dict[str, str]) -> Dict[str, str]:
        candidate_path = f"{MINIO_BUCKET}/{indices[CANDIDATE_KEY]}/*.csv"

        assets_path = indices.get(CANDIDATE_ASSETS_KEY, '')
        if assets_path:
            assets_path = f"{MINIO_BUCKET}/{assets_path}/*.csv"

        cancellation_reason_path = indices.get(CANCELLATION_REASON_KEY, '')
        if cancellation_reason_path:
            cancellation_reason_path = f"{MINIO_BUCKET}/{cancellation_reason_path}/*.csv"

        return {
            'spark.aletheia.buckets.candidate': candidate_path,
            'spark.aletheia.buckets.assets': assets_path,
            'spark.aletheia.buckets.cancellation_reason': cancellation_reason_path,

            'spark.es.resource': indices[INDEX_KEY],
        }

    SparkSubmitWithCredentialsOperator.partial(
        max_active_tis_per_dag=4,
        retries=2,
        retry_delay=timedelta(seconds=300),
        do_xcom_push=False,
        task_id='candidates_spark_job',
        application=(Path(__file__).parent.parent / 'spark' / 'candidates.py').as_posix(),
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
    ).expand(conf=format_spark_conf.expand(indices=indices))
