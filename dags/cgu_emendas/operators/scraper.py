from typing import Dict
import httpx
from airflow.decorators import task


BASE_URL = 'https://www.portaldatransparencia.gov.br/download-de-dados/emendas-parlamentares/UNICO'


@task(multiple_outputs=False)
def emendas_parlamentares() -> Dict[str, str]:
    with httpx.Client(timeout=30) as client:
        r = client.head(BASE_URL)  # Make sure that the file exists
        r.raise_for_status()

    return BASE_URL
