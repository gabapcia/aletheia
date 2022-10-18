from pendulum import datetime
from pendulum.tz import timezone
from airflow.decorators import dag

from cgu_emendas.operators.scraper import emendas_parlamentares
from cgu_emendas.operators.file_storage import download
from cgu_emendas.operators.database import elasticsearch
from cgu_emendas.operators.processing import memory


@dag(
    description='Emendas Parlamentares da Controladoria-Geral da União',
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2022, 1, 1, tz=timezone('UTC')),
    schedule_interval='@daily',
    tags=['CGU', 'PF'],
    default_args={},
)
def cgu_emendas():
    uri = emendas_parlamentares()

    filepath = download(uri)

    es_indice = elasticsearch()
    filepath >> es_indice

    memory(filepath, es_indice)


# Register DAG
dag = cgu_emendas()
