import re
import json
from datetime import date
from typing import Dict
import httpx
from unidecode import unidecode
from bs4 import BeautifulSoup
from airflow.decorators import task


BASE_URL = 'https://www.portaldatransparencia.gov.br'
CGU_PEP_URI = f'{BASE_URL}/download-de-dados/pep'

GENERATED_AT_KEY = 'generated_at'
FILE_URI_KEY = 'uri'


@task(multiple_outputs=True)
def peps() -> Dict[str, str]:
    with httpx.Client(timeout=5) as client:
        r = client.get(CGU_PEP_URI)
        r.raise_for_status()

    soup = BeautifulSoup(r.content, 'html.parser')
    script = None
    for s in soup.find_all('script'):
        text = unidecode(str(s))
        if 'var arquivos = ' in text:
            script = text
            break

    data = re.search(r'arquivos.push\((?P<data>.+)\)', script).group('data')
    data = json.loads(data)

    year = data['ano']
    month = data['mes']

    data = {
        GENERATED_AT_KEY: date(int(year), int(month), 1).isoformat(),
        FILE_URI_KEY: f'{BASE_URL}/download-de-dados/pep/{year}{month}',
    }

    return data
