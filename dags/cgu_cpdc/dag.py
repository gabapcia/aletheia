from datetime import timedelta
from pendulum import datetime
from pendulum.tz import timezone
from airflow.decorators import dag

from include.xcom_handler import drop_null
from cgu_cpdc.operators.scraper import cpdc
from cgu_cpdc.operators.idempotence import save_filedate
from cgu_cpdc.operators.file_storage import download
from cgu_cpdc.operators.database import elasticsearch
from cgu_cpdc.operators.processing import memory


@dag(
    description='Cartão de Pagamento da Defesa Civil (CPDC) da Controladoria-Geral da União',
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2022, 1, 1, tz=timezone('UTC')),
    schedule_interval='@daily',
    tags=['CGU', 'PF', 'PJ'],
    default_args={},
)
def cgu_cpdc():
    links_task = cpdc()

    idempotence_tasks = save_filedate.expand(link=links_task)

    download_tasks = download.override(
        retries=10,
        retry_delay=timedelta(seconds=10),
    ).expand(link=drop_null.override(task_id='drop_files_already_downloaded')(data=idempotence_tasks))

    files = elasticsearch.expand(file_data=download_tasks)

    memory.override(
        task_id='in_memory_processing',
        retries=5,
        retry_delay=timedelta(seconds=10),
        max_active_tis_per_dag=4,
    ).expand(file_data=files)


# Register DAG
dag = cgu_cpdc()
