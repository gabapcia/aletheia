import asyncio
import json
import re
from dataclasses import dataclass
from datetime import date
import httpx
from unidecode import unidecode
from bs4 import BeautifulSoup


@dataclass
class Response:
    generated_at: date
    payment_uri: str
    withdraw_uri: str


class Client:
    BASE_URL = 'http://www.portaldatransparencia.gov.br/'
    
    PAYMENT_INITIAL_URI = BASE_URL + '/download-de-dados/bolsa-familia-pagamentos'
    PAYMENT_DOWNLOAD_URI = BASE_URL + '/download-de-dados/bolsa-familia-pagamentos/{year}{month}'

    WITHDRAW_INITIAL_URI = BASE_URL + '/download-de-dados/bolsa-familia-saques'
    WITHDRAW_DOWNLOAD_URI = BASE_URL + '/download-de-dados/bolsa-familia-saques/{year}{month}'

    async def summary(self) -> list[Response]:
        payment = self._summary(Client.PAYMENT_INITIAL_URI, Client.PAYMENT_DOWNLOAD_URI)
        withdraw = self._summary(Client.WITHDRAW_INITIAL_URI, Client.WITHDRAW_DOWNLOAD_URI)

        payment, withdraw = await asyncio.gather(payment, withdraw)

        files = []
        for file_date in withdraw.keys():
            r = Response(
                generated_at=file_date,
                payment_uri=payment[file_date],
                withdraw_uri=withdraw[file_date],
            )
            files.append(r)

        return files

    async def _summary(self, uri: str, download_uri: str) -> dict[date, str]:
        async with httpx.AsyncClient() as client:
            r = await client.get(uri, timeout=5)

        soup = BeautifulSoup(r.content, 'html.parser')

        for script in soup.find_all('script'):
            text = unidecode(str(script))
            if 'var arquivos =' in text:
                break

        links = {}
        for match in re.finditer(r'arquivos\.push\((?P<data>[^\)]+)\)', text):
            data = json.loads(match.group('data'))

            file_date = date(year=int(data['ano']), month=int(data['mes']), day=1)
            file_uri = download_uri.format(year=data['ano'], month=data['mes'])

            links[file_date] = file_uri

        return links
