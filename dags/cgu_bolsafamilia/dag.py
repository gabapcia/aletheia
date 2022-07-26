from datetime import timedelta
from pendulum import datetime
from pendulum.tz import timezone
from airflow.decorators import dag

from include.xcom_handler import drop_null
from cgu_bolsafamilia.operators.scraper import bolsa_familia
from cgu_bolsafamilia.operators.idempotence import save_filedate
from cgu_bolsafamilia.operators.file_storage import download, extract, delete_folder
from cgu_bolsafamilia.operators.database import elasticsearch
from cgu_bolsafamilia.operators.processing import spark


@dag(
    description='Beneficiários do programa Bolsa Familia',
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2022, 1, 1, tz=timezone('UTC')),
    schedule_interval='@daily',
    tags=['CGU', 'PF'],
    default_args={},
)
def cgu_bolsafamilia():
    links = bolsa_familia()

    idempotence = save_filedate.expand(links=links)

    download_zips = download.override(
        retries=10,
        retry_delay=timedelta(seconds=10),
    ).expand(
        links=drop_null.override(task_id='drop_files_already_downloaded')(data=idempotence)
    )

    extract_files = extract.expand(download_zips=download_zips)

    indices = elasticsearch.expand(files=extract_files)

    spark_group = spark(indices)

    spark_group >> delete_folder.expand(folders=extract_files)


# Register DAG
dag = cgu_bolsafamilia()
