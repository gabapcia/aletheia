from pathlib import Path
from typing import Dict, List
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task, task_group
from spark_plugin.operators.spark import SparkSubmitWithCredentialsOperator
from spark_plugin.utils.lookup import ConfFromConnection as SparkConfFromConnection, ConfFromXCom as SparkConfFromXCom
from rfb_cnpj.operators.scraper import GENERATED_AT_KEY, SINGLE_FILES
from rfb_cnpj.operators.file_storage import MINIO_BUCKET, FILE_TYPE_KEY, ROOT_FOLDER_KEY, EXTRACTED_FILE_KEY


@task(multiple_outputs=False)
def config_from_catalog(catalog: List[Dict[str, str]], indice: str) -> Dict[str, str]:
    groupped_catalog = dict()

    for c in catalog:
        if c[FILE_TYPE_KEY] in SINGLE_FILES:
            groupped_catalog[c[FILE_TYPE_KEY]] = f'{MINIO_BUCKET}{c[EXTRACTED_FILE_KEY]}'.replace('$', '\$')  # noqa: W605
        elif c[FILE_TYPE_KEY] not in groupped_catalog:
            groupped_catalog[c[FILE_TYPE_KEY]] = f'{MINIO_BUCKET}{c[ROOT_FOLDER_KEY]}/*'.replace('$', '\$')  # noqa: W605

    config = {
        f'spark.aletheia.buckets.{k}': v
        for k, v in groupped_catalog.items()
    }

    config['spark.aletheia.filedate'] = c[GENERATED_AT_KEY]
    config['spark.es.resource'] = indice

    return groupped_catalog


@task_group
def spark(indice: str, catalog: List[Dict[str, str]]) -> TaskGroup:
    config = config_from_catalog(catalog=catalog, indice=indice)

    SparkSubmitWithCredentialsOperator.partial(
        task_id='companies_spark',
        application=(Path(__file__).parent.parent / 'spark' / 'companies.py').as_posix(),
        verbose=False,
        conn_id='spark_default',
        executor_memory='5G',
        total_executor_cores=8,
        lazy_conf={
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.access.key': SparkConfFromConnection(conn_id='minio_default', field='login'),
            'spark.hadoop.fs.s3a.secret.key': SparkConfFromConnection(conn_id='minio_default', field='password'),
            'spark.hadoop.fs.s3a.endpoint': SparkConfFromConnection(conn_id='minio_default', field=['schema', 'host', 'port'], format='{schema}://{host}:{port}'),

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
            'org.elasticsearch:elasticsearch-spark-30_2.12:8.4.3'
        ],
    ).expand(conf=config)
