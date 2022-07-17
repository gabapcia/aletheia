from pathlib import Path
from typing import Any, Dict, List
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
from minio_plugin.operators.extract_file import ExtractFileOperator
from spark_plugin.operators.spark import SparkSubmitWithCredentialsOperator
from spark_plugin.utils.lookup import ConfFromConnection as SparkConfFromConnection, ConfFromXCom as SparkConfFromXCom
from cgu_pep.operators.storage import MINIO_BUCKET
from cgu_pep.operators.database import PEOPLE_INDEX_KEY


@task
def get_bucket_path_wildcard(paths: List[str]) -> str:
    path = '/'.join(paths[0].split('/')[:-1])

    if path[0] == '/':
        path = path[1:]

    if path[-1] != '/':
        path += '/'

    path += '*'

    return f'{MINIO_BUCKET}/{path}'


def spark(indices: Dict[str, Any], extracted_file: ExtractFileOperator) -> TaskGroup:
    with TaskGroup(group_id='processing') as processing:
        with TaskGroup(group_id='paths') as paths:
            people = get_bucket_path_wildcard.override(task_id='get_people_filepath')(extracted_file.output)

        task = SparkSubmitWithCredentialsOperator(
            task_id=PEOPLE_INDEX_KEY,
            application=(Path(__file__).parent.parent / 'spark' / f'{PEOPLE_INDEX_KEY}.py').as_posix(),
            verbose=False,
            conn_id='spark_default',
            executor_memory='5G',
            conf={
                'spark.aletheia.buckets.people': SparkConfFromXCom(people, lookup=[]),

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
                'spark.es.resource': SparkConfFromXCom(indices[PEOPLE_INDEX_KEY], lookup=[]),
            },
            packages=[
                'com.amazonaws:aws-java-sdk-pom:1.12.164',
                'org.apache.hadoop:hadoop-aws:3.3.1',
                'org.elasticsearch:elasticsearch-spark-30_2.12:8.3.2',
            ],
        )
        paths >> task

    return processing
