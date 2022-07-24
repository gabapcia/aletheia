from pendulum import datetime
from pendulum.tz import timezone
from airflow.decorators import dag


from cgu_pep.operators.scraper import peps, GENERATED_AT_KEY
from cgu_pep.operators.idempotence import save_filedate
from cgu_pep.operators.file_storage import download, extract, delete_files, ROOT_FOLDER_KEY, TASK_KEY
from cgu_pep.operators.database import elasticsearch
from cgu_pep.operators.processing import spark


@dag(
    description='Pessoas Expostas Politicamente (PEP) from Controladoria-Geral da União',
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2022, 1, 1, tz=timezone('UTC')),
    schedule_interval='@daily',
    tags=['CGU'],
    default_args={},
)
def cgu_pep():
    links = peps()

    idempotence = save_filedate(links[GENERATED_AT_KEY])

    download_job = download(links)

    extract_job = extract(links, download_job[TASK_KEY])

    indice_task = elasticsearch()

    spark_group = spark(indice_task, extract_job[TASK_KEY])

    delete_extracted_files = delete_files(extract_job[ROOT_FOLDER_KEY])

    links >> idempotence >> download_job[TASK_KEY] >> extract_job[TASK_KEY] >> indice_task
    indice_task >> spark_group >> delete_extracted_files


# Register DAG
dag = cgu_pep()
