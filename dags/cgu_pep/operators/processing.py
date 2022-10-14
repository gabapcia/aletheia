from pathlib import Path
from typing import List
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
from minio_plugin.operators.extract_file import ExtractFileOperator
from spark_plugin.operators.spark import SparkSubmitWithCredentialsOperator
from spark_plugin.utils.lookup import ConfFromConnection as SparkConfFromConnection, ConfFromXCom as SparkConfFromXCom
from cgu_pep.operators.file_storage import MINIO_BUCKET


@task
def get_bucket_path_wildcard(paths: List[str]) -> str:
    path = '/'.join(paths[0].split('/')[:-1])

    if path[0] == '/':
        path = path[1:]

    if path[-1] != '/':
        path += '/'

    path += '*'

    return f'{MINIO_BUCKET}/{path}'


def spark(indice: str, extracted_file: ExtractFileOperator) -> TaskGroup:
    with TaskGroup(group_id='processing') as processing:
        get_bucket_path = get_bucket_path_wildcard.override(task_id='get_people_filepath')(extracted_file.output)

        process_files = SparkSubmitWithCredentialsOperator(
            task_id='people',
            application=(Path(__file__).parent.parent / 'spark' / 'people.py').as_posix(),
            verbose=False,
            conn_id='spark_default',
            executor_memory='1G',
            total_executor_cores=1,
            lazy_conf={
                'spark.aletheia.buckets.people': SparkConfFromXCom(get_bucket_path, lookup=[]),

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
                'spark.es.resource': SparkConfFromXCom(indice, lookup=[]),
            },
            packages=[
                'com.amazonaws:aws-java-sdk-pom:1.12.319',
                'org.apache.hadoop:hadoop-aws:3.3.1',
                'org.elasticsearch:elasticsearch-spark-30_2.12:8.4.3',
            ],
        )

        get_bucket_path >> process_files

    return processing
