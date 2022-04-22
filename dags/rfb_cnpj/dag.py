from pathlib import Path
from pendulum import datetime
from pendulum.tz import timezone
from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator

from cassandra_plugin.operators.keyspace import CreateKeyspaceOperator
from cassandra_plugin.operators.table import CreateTableOperator

from minio_plugin.operators.http_file_to_minio import FileDownloadOperator
from minio_plugin.operators.extract_to_minio import ExtractFileOperator
from minio_plugin.operators.delete_folder import DeleteFolderOperator
from minio_plugin.utils.lookup import (
    XComArgLookup as MinioXComArgLookup,
    FolderLookup as MinioFolderLookup,
)

from spark_plugin.operators.spark import SparkSubmitWithCredentialsOperator
from spark_plugin.utils.lookup import (
    ConfFromConnection as SparkConfFromConnection,
)

from rfb_cnpj.engine.scraper import scraper
from rfb_cnpj.engine.scraper import NUMBER_OF_URIS, FILE_TYPES, SINGLE_FILES


@dag(
    description='CNPJs from Receita Federal do Brasil',
    catchup=False,
    start_date=datetime(2022, 1, 1, tz=timezone('UTC')),
    schedule_interval='@daily',
    default_args={},
)
def rfb_cnpj():
    links = scraper()


    check_if_already_exists = DummyOperator(task_id='check_if_already_exists')


    with TaskGroup(group_id='download') as download:
        for type_ in FILE_TYPES:
            with TaskGroup(group_id=type_):
                for i in range(NUMBER_OF_URIS):
                    filedownload = FileDownloadOperator(
                        task_id=f'{i}',
                        uri=MinioXComArgLookup(raw=links, lookup=[type_, i]),
                        folder=MinioFolderLookup(raw=links, lookup=['generated_at'], path=f'{{raw}}/{type_}'),
                        minio_conn_id='minio_default',
                    )

                    fileextract = ExtractFileOperator(
                        task_id=f'{i}.extract',
                        zip=MinioXComArgLookup(raw=filedownload.output, lookup=list()),
                        folder=f'extracted/{type_}',
                        minio_conn_id='minio_default',
                    )

                    filedownload >> fileextract

        for type_ in SINGLE_FILES:
            filedownload = FileDownloadOperator(
                task_id=f'{type_}',
                uri=MinioXComArgLookup(raw=links, lookup=[type_]),
                folder=MinioFolderLookup(raw=links, lookup=['generated_at']),
                minio_conn_id='minio_default',
            )

            fileextract = ExtractFileOperator(
                task_id=f'{type_}.extract',
                zip=MinioXComArgLookup(raw=filedownload.output, lookup=list()),
                folder='extracted/single',
                filename=f'{type_}',
                minio_conn_id='minio_default',
            )

            filedownload >> fileextract


    with TaskGroup(group_id='cassandra') as cassandra:
        create_keyspace = CreateKeyspaceOperator(
            task_id='create_keyspace',
            keyspace='rfb_cnpj',
            rf=1,
            conn_id='cassandra_default',
        )

        create_company_table = CreateTableOperator(
            task_id='create_company_table',
            keyspace='rfb_cnpj',
            table='company',
            fields=dict(
                base_cnpj='text',
                cnpj='text',
                type_code='int',
                type='text',
                trading_name='text',
                situation_code='int',
                situation='text',
                date_situation='date',
                reason_situation_code='text',
                reason_situation='text',
                foreign_city_name='text',
                country_code='text',
                country='text',
                start_date='date',
                cnae='text',
                cnae_description='text',
                other_cnaes='map<text, text>',
                address='text',
                number='text',
                complement='text',
                district='text',
                zip_code='text',
                federative_unit='text',
                county_code='text',
                county='text',
                phone_1='text',
                phone_2='text',
                fax='text',
                email='text',
                special_situation='text',
                date_special_situation='date',
                corporate_name='text',
                legal_nature_code='text',
                legal_nature='text',
                responsible_qualification_code='text',
                responsible_qualification='text',
                share_capital='bigint',
                size_code='text',
                size='text',
                responsible_federative_entity='text',
                opted_for_simples='text',
                date_opted_for_simples='date',
                simples_exclusion_date='date',
                opted_for_mei='text',
                date_opted_for_mei='date',
                mei_exclusion_date='date',
            ),
            partition=['base_cnpj'],
            primary_key=['cnpj'],
            indexes=[],
            conn_id='cassandra_default',
        )

        create_partner_table = CreateTableOperator(
            task_id='create_partner_table',
            keyspace='rfb_cnpj',
            table='partner',
            fields=dict(
                base_cnpj='text',
                type_code='text',
                type='text',
                name='text',
                tax_id='text',
                qualification_code='text',
                qualification='text',
                join_date='date',
                country_code='text',
                country='text',
                legal_representative_name='text',
                legal_representative_tax_id='text',
                legal_representative_qualification_code='text',
                legal_representative_qualification='text',
                age_group_code='text',
                age_group='text',
            ),
            partition=['base_cnpj'],
            primary_key=[
                'type_code',
                'name',
                'tax_id',
                'join_date',
                'country_code',
                'legal_representative_name',
                'legal_representative_tax_id',
            ],
            conn_id='cassandra_default',
        )

        create_keyspace >> [create_company_table, create_partner_table]


    with TaskGroup(group_id='processing') as processing:
        for type_ in ['company', 'partner']:
            SparkSubmitWithCredentialsOperator(
                task_id=type_,
                application=(Path(__file__).parent / 'spark' / f'{type_}.py').as_posix(),
                verbose=False,
                conn_id='spark_default',
                conf={
                    'spark.hadoop.fs.s3a.access.key': SparkConfFromConnection(
                        conn_id='minio_default',
                        field='login'
                    ),
                    'spark.hadoop.fs.s3a.secret.key': SparkConfFromConnection(
                        conn_id='minio_default',
                        field='password'
                    ),
                    'spark.hadoop.fs.s3a.endpoint': SparkConfFromConnection(
                        conn_id='minio_default',
                        field=['host', 'port'],
                        format='http://{host}:{port}',
                    ),
                    'spark.hadoop.fs.s3a.path.style.access': 'true',
                    'spark.cassandra.connection.host': SparkConfFromConnection(
                        conn_id='cassandra_default',
                        field='host',
                    ),
                    'spark.sql.extensions': 'com.datastax.spark.connector.CassandraSparkExtensions',
                },
                packages=[
                    'com.amazonaws:aws-java-sdk-pom:1.12.164',
                    'org.apache.hadoop:hadoop-aws:3.3.1',
                    'com.datastax.spark:spark-cassandra-connector_2.12:3.1.0',
                ],
            )


    delete_extracted_files = DeleteFolderOperator(
        task_id='delete_extracted_files',
        folder='extracted/',
        minio_conn_id='minio_default',
    )


    links >> check_if_already_exists >> download >> cassandra >> processing >> delete_extracted_files


# Register DAG
dag = rfb_cnpj()
