from pendulum import datetime
from pendulum.tz import timezone
from airflow.decorators import dag

from minio_plugin.operators.delete_folder import DeleteFolderOperator

from cgu_pep.operators.scraper import peps, GENERATED_AT_KEY
from cgu_pep.operators.idempotence import save_filedate
from cgu_pep.operators.storage import download, extract, MINIO_BUCKET, ROOT_FOLDER_KEY, TASK_KEY
from cgu_pep.operators.database import elasticsearch
from cgu_pep.operators.processing import spark



@dag(
    description='Pessoas Expostas Politicamente (PEP) from Controladoria-Geral da União',
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2022, 1, 1, tz=timezone('UTC')),
    schedule_interval='@daily',
    tags=[],
    default_args={},
)
def cgu_pep():
    links = peps()

    idempotence = save_filedate(links[GENERATED_AT_KEY])

    download_group, download_task = download(links)

    extract_group, extract_task = extract(links, download_task[TASK_KEY])

    database_group, indice_tasks = elasticsearch()

    spark_group = spark(indice_tasks, extract_task[TASK_KEY])

    delete_extracted_files = DeleteFolderOperator(
        task_id='delete_extracted_files',
        bucket=MINIO_BUCKET,
        folder=extract_task[ROOT_FOLDER_KEY],
        minio_conn_id='minio_default',
    )

    links >> idempotence >> download_group >> extract_group >> database_group >> spark_group >> delete_extracted_files


# Register DAG
dag = cgu_pep()
