from pendulum import datetime
from pendulum.tz import timezone
from airflow.decorators import dag

from minio_plugin.operators.delete_folder import DeleteFolderOperator

from rfb_cnpj.operators.download import MINIO_BUCKET
from rfb_cnpj.operators.scraper import cnpjs, GENERATED_AT_KEY
from rfb_cnpj.operators.idempotence import save_filedate
from rfb_cnpj.operators.download import download
from rfb_cnpj.operators.extract import extract
from rfb_cnpj.operators.database import elasticsearch
from rfb_cnpj.operators.processing import spark


@dag(
    description='CNPJs da Receita Federal do Brasil',
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2022, 1, 1, tz=timezone('UTC')),
    schedule_interval='@daily',
    tags=['RFB', 'PF', 'PJ'],
    default_args={},
)
def rfb_cnpj():
    links = cnpjs()

    check_if_already_exists = save_filedate(links[GENERATED_AT_KEY])

    download_group, download_tasks = download(links)

    extract_group, extract_tasks = extract(links, download_tasks)

    elasticsearch_group, indices_tasks = elasticsearch()

    spark_group = spark(indices_tasks, extract_tasks)

    delete_extracted_files = DeleteFolderOperator(
        task_id='delete_extracted_files',
        bucket=MINIO_BUCKET,
        folder=extract_tasks['root_folder'],
        minio_conn_id='minio_default',
    )

    links >> check_if_already_exists >> download_group >> extract_group
    extract_group >> elasticsearch_group >> spark_group >> delete_extracted_files


# Register DAG
dag = rfb_cnpj()
