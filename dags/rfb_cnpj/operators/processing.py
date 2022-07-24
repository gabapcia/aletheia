from pathlib import Path
from typing import Any, Dict, List
from airflow import XComArg
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
from spark_plugin.operators.spark import SparkSubmitWithCredentialsOperator
from spark_plugin.utils.lookup import ConfFromConnection as SparkConfFromConnection, ConfFromXCom as SparkConfFromXCom
from rfb_cnpj.operators.extract import MINIO_BUCKET
from rfb_cnpj.operators.database import COMPANIES_INDEX_KEY, PARTNERS_INDEX_KEY
from rfb_cnpj.operators.scraper import (
    PARTNER_KEY,
    COMPANY_KEY,
    BRANCH_KEY,
    SIMPLES_KEY,
    CNAE_INFO_KEY,
    REGISTER_SITUATION_KEY,
    COUNTY_KEY,
    LEGAL_NATURE_KEY,
    COUNTRY_KEY,
    PARTNER_QUALIFICATION_KEY,
)


@task
def get_bucket_path_wildcard(paths: List[List[str]]) -> str:
    paths = [path for task in paths for path in task]

    path = '/'.join(paths[0].split('/')[:-1])

    if path[0] == '/':
        path = path[1:]

    if path[-1] != '/':
        path += '/'

    path += '*'

    return f'{MINIO_BUCKET}/{path}'.replace('$', '\$')  # noqa: W605


@task
def get_bucket_path(path: List[str]) -> str:
    path = path[0]

    if path[0] == '/':
        path = path[1:]

    return f'{MINIO_BUCKET}/{path}'.replace('$', '\$')  # noqa: W605


def spark(indices: Dict[str, Any], extracted_files: Dict[str, Any]) -> TaskGroup:
    with TaskGroup(group_id='processing') as processing:
        with TaskGroup(group_id='paths') as paths:
            companies = get_bucket_path_wildcard.override(task_id='get_companies_filepath')(XComArg(extracted_files[COMPANY_KEY]))
            branches = get_bucket_path_wildcard.override(task_id='get_branches_filepath')(XComArg(extracted_files[BRANCH_KEY]))
            partners = get_bucket_path_wildcard.override(task_id='get_partners_filepath')(XComArg(extracted_files[PARTNER_KEY]))

            country = get_bucket_path.override(task_id='get_country_filepath')(extracted_files[COUNTRY_KEY].output)
            county = get_bucket_path.override(task_id='get_county_filepath')(extracted_files[COUNTY_KEY].output)
            register_situation = get_bucket_path.override(task_id='get_register_situation_filepath')(extracted_files[REGISTER_SITUATION_KEY].output)
            cnae_info = get_bucket_path.override(task_id='get_cnae_info_filepath')(extracted_files[CNAE_INFO_KEY].output)
            legal_nature = get_bucket_path.override(task_id='get_legal_nature_filepath')(extracted_files[LEGAL_NATURE_KEY].output)
            partner_qualification = get_bucket_path.override(task_id='get_partner_qualification_filepath')(extracted_files[PARTNER_QUALIFICATION_KEY].output)
            simples = get_bucket_path.override(task_id='get_simples_filepath')(extracted_files[SIMPLES_KEY].output)

        for filetype in [COMPANIES_INDEX_KEY, PARTNERS_INDEX_KEY]:
            op = SparkSubmitWithCredentialsOperator(
                task_id=filetype,
                application=(Path(__file__).parent.parent / 'spark' / f'{filetype}.py').as_posix(),
                verbose=False,
                conn_id='spark_default',
                executor_memory='5G',
                total_executor_cores=8,
                lazy_conf={
                    'spark.aletheia.buckets.company': SparkConfFromXCom(companies, lookup=[]),
                    'spark.aletheia.buckets.branch': SparkConfFromXCom(branches, lookup=[]),
                    'spark.aletheia.buckets.partner': SparkConfFromXCom(partners, lookup=[]),
                    'spark.aletheia.buckets.country': SparkConfFromXCom(country, lookup=[]),
                    'spark.aletheia.buckets.county': SparkConfFromXCom(county, lookup=[]),
                    'spark.aletheia.buckets.register_situation': SparkConfFromXCom(register_situation, lookup=[]),
                    'spark.aletheia.buckets.cnae_info': SparkConfFromXCom(cnae_info, lookup=[]),
                    'spark.aletheia.buckets.legal_nature': SparkConfFromXCom(legal_nature, lookup=[]),
                    'spark.aletheia.buckets.partner_qualification': SparkConfFromXCom(partner_qualification, lookup=[]),
                    'spark.aletheia.buckets.simples': SparkConfFromXCom(simples, lookup=[]),

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
                    'spark.es.resource': SparkConfFromXCom(indices[filetype], lookup=[]),
                },
                packages=[
                    'com.amazonaws:aws-java-sdk-pom:1.12.164',
                    'org.apache.hadoop:hadoop-aws:3.3.1',
                    'org.elasticsearch:elasticsearch-spark-30_2.12:8.3.2'
                ],
            )
            paths >> op

    return processing
