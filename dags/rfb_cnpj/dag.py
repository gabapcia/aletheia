from datetime import timedelta
from pendulum import datetime
from pendulum.tz import timezone
from airflow.decorators import dag

from rfb_cnpj.operators.scraper import cnpjs
from rfb_cnpj.operators.idempotence import idempotence
from rfb_cnpj.operators.file_storage import flat_catalog, download, extract, delete_folder
from rfb_cnpj.operators.database import elasticsearch
from rfb_cnpj.operators.processing import spark


@dag(
    description='CNPJs da Receita Federal do Brasil',
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2022, 1, 1, tz=timezone('UTC')),
    schedule='@daily',
    tags=['RFB', 'PF', 'PJ'],
    default_args={},
)
def rfb_cnpj():
    catalog = cnpjs()

    idempotence_task = idempotence(data=catalog)

    download_tasks = (
        download
        .override(
            retries=10,
            retry_delay=timedelta(seconds=30),
            max_active_tis_per_dag=6,
        )
        .expand(data=flat_catalog(catalog=catalog))
    )
    idempotence_task >> download_tasks

    extract_tasks = extract.expand(file_info=download_tasks)

    elasticsearch_task = elasticsearch()
    extract_tasks >> elasticsearch_task

    spark_group = spark(elasticsearch_task, extract_tasks)

    delete_extracted_files_task = delete_folder(catalog=extract_tasks)
    spark_group >> delete_extracted_files_task


# Register DAG
dag = rfb_cnpj()
