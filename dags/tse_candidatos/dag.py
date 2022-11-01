from datetime import timedelta
from pendulum import datetime
from pendulum.tz import timezone
from airflow.decorators import dag

from tse_candidatos.operators.scraper import candidatos
from tse_candidatos.operators.idempotence import idempotence
from tse_candidatos.operators.file_storage import flat_catalog, group_files, download, extract, delete_folder
from tse_candidatos.operators.database import elasticsearch
from tse_candidatos.operators.processing import spark


@dag(
    description='Candidatos das Eleições - Tribunal Superior Eleitoral',
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2022, 1, 1, tz=timezone('UTC')),
    schedule='@daily',
    tags=['TSE', 'PF'],
    default_args={},
)
def tse_candidatos():
    links_task = candidatos()
    catalog_task = idempotence(catalog=links_task)

    download_tasks = (
        download
        .override(
            retries=10,
            retry_delay=timedelta(seconds=10),
        )
        .expand(data=flat_catalog(catalog=catalog_task))
    )

    extract_tasks = extract.expand(file_info=download_tasks)

    files = group_files(file_list=extract_tasks)

    database_tasks = elasticsearch.expand(file_data=files)

    processing_tasks = spark(indices=database_tasks)

    delete_tasks = delete_folder.expand(folders=extract_tasks)

    processing_tasks >> delete_tasks


# Register DAG
dag = tse_candidatos()
