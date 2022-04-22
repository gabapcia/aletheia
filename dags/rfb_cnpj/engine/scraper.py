import re
from dataclasses import dataclass, asdict
from datetime import date, datetime
from typing import Any, Dict, List
import httpx
from unidecode import unidecode
from bs4 import BeautifulSoup
from airflow.decorators import task


NUMBER_OF_URIS = 10
FILE_TYPES = ['company', 'branch', 'partner']
SPECIAL_FILES = ['tax_regime']
SINGLE_FILES = [
    'simples_info',
    'cnae_info_map',
    'register_situation_map',
    'county_map',
    'legal_nature_info',
    'country_map',
    'partner_qualification_map',
]


@dataclass(frozen=True)
class Response:
    generated_at: date
    partner: List[str]
    company: List[str]
    branch: List[str]
    simples_info: str
    cnae_info_map: str
    register_situation_map: str
    county_map: str
    legal_nature_info: str
    country_map: str
    partner_qualification_map: str
    tax_regime: str

    def json(self) -> Dict[str, Any]:
        data = asdict(self)
        data['generated_at'] = data['generated_at'].isoformat()
        return data


class Client:
    BASE_URL = 'https://www.gov.br/receitafederal/pt-br/'
    INITIAL_URI = BASE_URL + 'assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj'

    def summary(self) -> Response:
        r = httpx.get(Client.INITIAL_URI, timeout=5)

        content = BeautifulSoup(r.content, 'html.parser').find(id='content-core')

        data = Response(
            generated_at=self._get_generated_at(content),
            partner=self._get_partner_uris(content),
            company=self._get_company_uris(content),
            branch=self._get_branch_uris(content),
            simples_info=self._get_uri_by_text(content, 'simples nacional/mei'),
            cnae_info_map=self._get_uri_by_text(content, 'atributo cnae'),
            register_situation_map=self._get_uri_by_text(content, 'motivo da situacao cadastral'),
            county_map=self._get_uri_by_text(content, 'atributo municipio'),
            legal_nature_info=self._get_uri_by_text(content, 'atributo natureza juridica'),
            country_map=self._get_uri_by_text(content, 'atributo pais'),
            partner_qualification_map=self._get_uri_by_text(content, 'atributo qualificacao dos socios'),
            tax_regime=self._get_uri_by_text(content, 'regime tributario')
        )
        return data

    def _get_generated_at(self, content: BeautifulSoup) -> date:
        date_regex = re.search(r'data da ultima extracao:(.+)', unidecode(content.text).lower().strip())
        if not date_regex:
            raise ValueError('It was not possible to extract the generation date from the files')

        generated_at = datetime.strptime(date_regex.groups()[0].strip(), '%d/%m/%Y').date()
        return generated_at

    def _get_company_uris(self, content: BeautifulSoup) -> List[str]:
        links = set()
        for a in content.find_all('a'):
            text = unidecode(a.text).strip().lower()
            if text.startswith('dados abertos cnpj empresa'):
                links.add(a['href'])

        return list(links)

    def _get_branch_uris(self, content: BeautifulSoup) -> List[str]:
        links = set()
        for a in content.find_all('a'):
            text = unidecode(a.text).strip().lower()
            if text.startswith('dados abertos cnpj estabelecimento'):
                links.add(a['href'])

        return list(links)

    def _get_partner_uris(self, content: BeautifulSoup) -> List[str]:
        links = set()
        for a in content.find_all('a'):
            text = unidecode(a.text).strip().lower()
            if text.startswith('dados abertos cnpj socio'):
                links.add(a['href'])

        return list(links)

    def _get_uri_by_text(self, content: BeautifulSoup, target: str) -> str:
        for a in content.find_all('a'):
            text = unidecode(a.text).strip().lower()
            if text.endswith(target):
                return a['href']

        raise ValueError(f'"{target}" file URI not found')


@task(task_id='get_urls_from_rfb')
def scraper() -> Dict[str, List[str]]:
    data = Client().summary()

    if (
        len(data.partner) != NUMBER_OF_URIS or
        len(data.company) != NUMBER_OF_URIS or
        len(data.branch) != NUMBER_OF_URIS
    ):
        raise ValueError('Too big')

    return data.json()
