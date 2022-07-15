import re
from datetime import date, datetime
from typing import Dict, List, Union
import httpx
from unidecode import unidecode
from bs4 import BeautifulSoup
from airflow.decorators import task


RFB_CNPJ_URI = 'https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj'  # noqa

GENERATED_AT_KEY = 'generated_at'
PARTNER_KEY = 'partner'
COMPANY_KEY = 'company'
BRANCH_KEY = 'branch'
SIMPLES_KEY = 'simples_info'
CNAE_INFO_KEY = 'cnae_info_map'
REGISTER_SITUATION_KEY = 'register_situation_map'
COUNTY_KEY = 'county_map'
LEGAL_NATURE_KEY = 'legal_nature_info'
COUNTRY_KEY = 'country_map'
PARTNER_QUALIFICATION_KEY = 'partner_qualification_map'
TAX_REGIME_KEY = 'tax_regime'

MAPPER_FILES = [
    SIMPLES_KEY,
    CNAE_INFO_KEY,
    REGISTER_SITUATION_KEY,
    COUNTY_KEY,
    LEGAL_NATURE_KEY,
    COUNTRY_KEY,
    PARTNER_QUALIFICATION_KEY,
    # TAX_REGIME_KEY,
]


def _get_generated_at(content: BeautifulSoup) -> date:
    date_regex = re.search(r'data da ultima extracao:(.+)', unidecode(content.text).lower().strip())
    if not date_regex:
        raise ValueError('It was not possible to extract the generation date from the files')

    generated_at = datetime.strptime(date_regex.groups()[0].strip(), '%d/%m/%Y').date()
    return generated_at


def _get_company_uris(content: BeautifulSoup) -> List[str]:
    links = set()
    for a in content.find_all('a'):
        text = unidecode(a.text).strip().lower()
        if text.startswith('dados abertos cnpj empresa'):
            links.add(a['href'])

    return list(links)


def _get_branch_uris(content: BeautifulSoup) -> List[str]:
    links = set()
    for a in content.find_all('a'):
        text = unidecode(a.text).strip().lower()
        if text.startswith('dados abertos cnpj estabelecimento'):
            links.add(a['href'])

    return list(links)


def _get_partner_uris(content: BeautifulSoup) -> List[str]:
    links = set()
    for a in content.find_all('a'):
        text = unidecode(a.text).strip().lower()
        if text.startswith('dados abertos cnpj socio'):
            links.add(a['href'])

    return list(links)


def _get_uri_by_text(content: BeautifulSoup, target: str) -> str:
    for a in content.find_all('a'):
        text = unidecode(a.text).strip().lower()
        if text.endswith(target):
            return a['href']

    raise ValueError(f'"{target}" file URI not found')


@task(multiple_outputs=True)
def cnpjs() -> Dict[str, Union[List[str], str]]:
    with httpx.Client(timeout=5) as client:
        r = client.get(RFB_CNPJ_URI)
        r.raise_for_status()

    content = BeautifulSoup(r.content, 'html.parser').find(id='content-core')

    data = {
        GENERATED_AT_KEY: _get_generated_at(content).isoformat(),
        PARTNER_KEY: _get_partner_uris(content),
        COMPANY_KEY: _get_company_uris(content),
        BRANCH_KEY: _get_branch_uris(content),
        SIMPLES_KEY: _get_uri_by_text(content, 'simples nacional/mei'),
        CNAE_INFO_KEY: _get_uri_by_text(content, 'atributo cnae'),
        REGISTER_SITUATION_KEY: _get_uri_by_text(content, 'motivo da situacao cadastral'),
        COUNTY_KEY: _get_uri_by_text(content, 'atributo municipio'),
        LEGAL_NATURE_KEY: _get_uri_by_text(content, 'atributo natureza juridica'),
        COUNTRY_KEY: _get_uri_by_text(content, 'atributo pais'),
        PARTNER_QUALIFICATION_KEY: _get_uri_by_text(content, 'atributo qualificacao dos socios'),
        TAX_REGIME_KEY: _get_uri_by_text(content, 'regime tributario'),
    }

    return data
