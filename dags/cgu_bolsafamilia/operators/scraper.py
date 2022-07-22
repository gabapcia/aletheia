import json
import re
from datetime import date
from typing import Dict
import httpx
from airflow.decorators import task
from unidecode import unidecode
from bs4 import BeautifulSoup


BASE_URL = 'https://www.portaldatransparencia.gov.br'
PAYMENT_INITIAL_URI = BASE_URL + '/download-de-dados/bolsa-familia-pagamentos'
PAYMENT_DOWNLOAD_URI = BASE_URL + '/download-de-dados/bolsa-familia-pagamentos/{year}{month}'
WITHDRAW_INITIAL_URI = BASE_URL + '/download-de-dados/bolsa-familia-saques'
WITHDRAW_DOWNLOAD_URI = BASE_URL + '/download-de-dados/bolsa-familia-saques/{year}{month}'


PAYMENT_KEY = 'payment'
WITHDRAW_KEY = 'withdraw'


def _get_download_link(uri: str, download_uri_template: str) -> Dict[date, str]:
    with httpx.Client(timeout=30) as client:
        r = client.get(uri)

    soup = BeautifulSoup(r.content, 'html.parser')

    for script in soup.find_all('script'):
        text = unidecode(str(script))
        if 'var arquivos =' in text:
            break

    links = {}
    for match in re.finditer(r'arquivos\.push\((?P<data>[^\)]+)\)', text):
        data = json.loads(match.group('data'))

        file_date = date(year=int(data['ano']), month=int(data['mes']), day=1)
        file_uri = download_uri_template.format(year=data['ano'], month=data['mes'])

        links[file_date] = file_uri

    return links


@task(multiple_outputs=False)
def bolsa_familia() -> Dict[str, Dict[str, str]]:
    payment = _get_download_link(PAYMENT_INITIAL_URI, PAYMENT_DOWNLOAD_URI)
    withdraw = _get_download_link(WITHDRAW_INITIAL_URI, WITHDRAW_DOWNLOAD_URI)

    data = {}
    for filedate, uri in payment.items():
        withdraw_uri = withdraw.get(filedate)
        if not withdraw_uri:
            continue

        data[filedate.isoformat()] = {
            PAYMENT_KEY: uri,
            WITHDRAW_KEY: withdraw_uri,
        }

    return data
