import json
import re
from dataclasses import dataclass
from datetime import date
import httpx
from unidecode import unidecode
from bs4 import BeautifulSoup


@dataclass
class Response:
    reference_date: date
    uri: str


class Client:
    BASE_URL = 'http://www.portaldatransparencia.gov.br'
    INITIAL_URI = BASE_URL + '/download-de-dados/garantia-safra'
    DOWNLOAD_URI = INITIAL_URI + '/{year}{month}'

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
            releases.append(Response(
                reference_date=date(year=int(release['ano']), month=int(release['mes']), day=1),
                uri=Client.DOWNLOAD_URI.format(year=release['ano'], month=release['mes']),
            ))

        return releases
