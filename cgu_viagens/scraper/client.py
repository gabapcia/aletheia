from dataclasses import dataclass
import re
import json
import httpx
from bs4 import BeautifulSoup
from unidecode import unidecode


@dataclass
class Response:
    year: int
    uri: str


class Client:
    BASE_URL = 'http://www.portaldatransparencia.gov.br'
    INITIAL_URI = BASE_URL + '/download-de-dados/viagens'
    FILE_URI = BASE_URL + '/download-de-dados/viagens/{year}'

    async def summary(self) -> list[Response]:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(Client.INITIAL_URI)

        data = None
        for script in BeautifulSoup(r.content, 'html.parser').find_all('script'):
            script = unidecode(str(script)).lower()
            if 'var arquivos = []' in script:
                data = script
                break

        releases = []
        for release in re.findall(r'arquivos.push\((?P<data>.+)\)', data):
            release = json.loads(release)
            response = Response(
                year=int(release['ano']),
                uri=Client.FILE_URI.format(year=release['ano']),
            )
            releases.append(response)

        return releases
