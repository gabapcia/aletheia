import re
from dataclasses import dataclass
from datetime import date, datetime
import httpx
from unidecode import unidecode
from bs4 import BeautifulSoup


@dataclass(frozen=True)
class Response:
    generated_at: date
    partner_uris: list[str]
    company_uris: list[str]
    company_place_uris: list[str]
    simples_mei_info: str


class Client:
    BASE_URL = 'https://www.gov.br/receitafederal/pt-br/'
    INITIAL_URI = BASE_URL + 'assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj'

    async def summary(self) -> Response:
        async with httpx.AsyncClient() as client:
            r = await client.get(Client.INITIAL_URI, timeout=5)

        content = BeautifulSoup(r.content, 'html.parser').find(id='content-core')
        response = Response(
            generated_at=self._get_generated_at(content),
            partner_uris=self._get_partner_uris(content),
            company_uris=self._get_company_uris(content),
            company_place_uris=self._get_company_place_uris(content),
            simples_mei_info=self._get_simples_mei_info(content),
        )
        return response

    def _get_generated_at(self, content: BeautifulSoup) -> date:
        for p in content.find_all('p'):
            text = unidecode(p.text).strip().lower()
            if text.startswith('data da ultima extracao'):
                return datetime.strptime(text.split(':')[-1].strip(), '%d/%m/%Y').date()

        raise ValueError('It was not possible to extract the generation date from the files')
                
    def _get_company_uris(self, content: BeautifulSoup) -> list[str]:
        links = []
        for a in content.find_all('a'):
            text = unidecode(a.text).strip().lower()
            if text.startswith('dados abertos cnpj empresa'):
                links.append(a['href'])

        return links

    def _get_company_place_uris(self, content: BeautifulSoup) -> list[str]:
        links = []
        for a in content.find_all('a'):
            text = unidecode(a.text).strip().lower()
            if text.startswith('dados abertos cnpj estabelecimento'):
                links.append(a['href'])

        return links

    def _get_partner_uris(self, content: BeautifulSoup) -> list[str]:
        links = []
        for a in content.find_all('a'):
            text = unidecode(a.text).strip().lower()
            if text.startswith('dados abertos cnpj socio'):
                links.append(a['href'])

        return links

    def _get_simples_mei_info(self, content: BeautifulSoup) -> str:
        for a in content.find_all('a'):
            text = unidecode(a.text).strip().lower()
            if text.startswith('informacoes sobre o simples nacional/mei'):
                return a['href']

        raise ValueError('"informacoes sobre o simples nacional/mei" file URI not found')


if __name__ == '__main__':
    import asyncio
    asyncio.run(Client().summary())