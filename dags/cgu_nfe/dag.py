from datetime import timedelta
from pendulum import datetime
from pendulum.tz import timezone
from airflow.decorators import dag

from include.xcom_handler import drop_null
from cgu_nfe.operators.scraper import nfe
from cgu_nfe.operators.idempotence import save_filedate
from cgu_nfe.operators.file_storage import download, extract, delete_folder
from cgu_nfe.operators.database import elasticsearch
from cgu_nfe.operators.processing import spark


@dag(
    description='Notas Fiscais da Controladoria-Geral da União',
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2022, 1, 1, tz=timezone('UTC')),
    schedule_interval='@daily',
    tags=['CGU', 'PF', 'PJ'],
    default_args={},
)
def cgu_nfe():
    links_task = nfe()

    idempotence_tasks = save_filedate.expand(link=links_task)

    download_tasks = download.override(
        retries=10,
        retry_delay=timedelta(seconds=10),
    ).expand(link=drop_null.override(task_id='drop_files_already_downloaded')(data=idempotence_tasks))

    extract_tasks = extract.expand(download_zips=download_tasks)

    indices = elasticsearch.expand(file_data=extract_tasks)

    spark_group = spark(indices=indices)

    delete_folder_tasks = delete_folder.expand(folders=indices)
    spark_group >> delete_folder_tasks


# Register DAG
dag = cgu_nfe()
