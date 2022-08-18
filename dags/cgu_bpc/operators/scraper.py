import re
import json
from datetime import date
from typing import Dict
import httpx
from unidecode import unidecode
from bs4 import BeautifulSoup
from airflow.decorators import task


BASE_URL = 'https://www.portaldatransparencia.gov.br/download-de-dados/bpc'


@task(multiple_outputs=False)
def bpc() -> Dict[str, str]:
    with httpx.Client(timeout=30) as client:
        r = client.get(BASE_URL)
        r.raise_for_status()

    soup = BeautifulSoup(r.content, 'html.parser')
    for s in soup.find_all('script'):
        text = unidecode(str(s))
        if 'var arquivos = ' in text:
            break

    links = {}
    for match in re.finditer(r'arquivos\.push\((?P<data>[^\)]+)\)', text):
        data = json.loads(match.group('data'))

        file_date = date(year=int(data['ano']), month=int(data['mes']), day=1)
        file_uri = f"{BASE_URL}/{data['ano']}{data['mes']}"

        links[file_date.isoformat()] = file_uri

    return links
