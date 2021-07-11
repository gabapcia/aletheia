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
    uri: str


class Client:
    BASE_URL = 'http://www.portaldatransparencia.gov.br'
    INITIAL_URI = BASE_URL + '/download-de-dados/pep'
    DOWNLOAD_URI = BASE_URL + '/download-de-dados/pep/{year}{month}'

    async def summary(self) -> Response:
        async with httpx.AsyncClient() as client:
            r = await client.get(Client.INITIAL_URI, timeout=5)

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

        response = Response(
            generated_at=date(int(year), int(month), 1),
            uri=Client.DOWNLOAD_URI.format(year=year, month=month)
        )
        return response
