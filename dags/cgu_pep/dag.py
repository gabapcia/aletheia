from pendulum import datetime
from pendulum.tz import timezone
from airflow.decorators import dag


from cgu_pep.operators.scraper import peps
from cgu_pep.operators.file_storage import idempotence, download
from cgu_pep.operators.database import elasticsearch
from cgu_pep.operators.processing import memory


@dag(
    description='Pessoas Expostas Politicamente (PEP) da Controladoria-Geral da União',
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2022, 1, 1, tz=timezone('UTC')),
    schedule='@daily',
    tags=['CGU', 'PF'],
    default_args={},
)
def cgu_pep():
    fileinfo = peps()

    filepath = download(info=fileinfo)

    idempotence_check = idempotence(info=fileinfo)
    idempotence_check >> filepath

    es_indice = elasticsearch()
    filepath >> es_indice

    memory(filepath=filepath, es_indice=es_indice)


# Register DAG
dag = cgu_pep()
