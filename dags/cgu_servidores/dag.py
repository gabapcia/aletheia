from datetime import timedelta
from pendulum import datetime
from pendulum.tz import timezone
from airflow.decorators import dag

from cgu_servidores.operators.scraper import servidores, RETIRED_KEY, PENSIONER_KEY, EMPLOYEE_KEY
from cgu_servidores.operators.idempotence import idempotence
from cgu_servidores.operators.file_storage import flat_catalog, group_files, download, extract, delete_folder
from cgu_servidores.operators.database import elasticsearch
from cgu_servidores.operators.processing import spark


@dag(
    description='Servidores Civis e Militares do Executivo Federal da Controladoria-Geral da União',
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2022, 1, 1, tz=timezone('UTC')),
    schedule_interval='@daily',
    tags=['CGU', 'PF'],
    default_args={},
)
def cgu_servidores():
    links = servidores()

    idempotence_task = idempotence(catalog=links)

    catalog_flattened = flat_catalog(catalog=idempotence_task)

    download_tasks = (
        download
        .override(
            retries=10,
            retry_delay=timedelta(seconds=10),
        )
        .expand(data=catalog_flattened)
    )
    extract_tasks = extract.expand(file_info=download_tasks)

    catalog = group_files(file_list=extract_tasks)

    delete_folder_tasks = delete_folder.expand(folders=extract_tasks)

    for filetype in [RETIRED_KEY, PENSIONER_KEY, EMPLOYEE_KEY]:
        indexed_catalog = elasticsearch(catalog=catalog, filetype=filetype)

        processing = spark(catalog=indexed_catalog, filetype=filetype)

        processing >> delete_folder_tasks


# Register DAG
dag = cgu_servidores()
