from typing import Any, Dict
import dateparser
import httpx
from unidecode import unidecode
from bs4 import BeautifulSoup
from airflow.decorators import task


BASE_URL = 'https://dadosabertos.tse.jus.br'
CATALOG_URI = f'{BASE_URL}/dataset/?groups=candidatos'

FILE_URI_KEY = 'uri'
FILE_CREATED_AT_KEY = 'created_at'
FILE_UPDATED_AT_KEY = 'updated_at'
FILE_METADATA_KEY = 'metadata'

CANDIDATE_KEY = 'candidate'  # Always required
CANDIDATE_ASSETS_KEY = 'assets'
# COALITIONS_KEY = 'coalitions'  # Always required
# VACANCIES_KEY = 'vacancies'  # Always required
CANCELLATION_REASON_KEY = 'cancellation_reason'
FILE_TYPE_KEYS = [
    CANDIDATE_KEY,
    CANDIDATE_ASSETS_KEY,
    # COALITIONS_KEY,
    # VACANCIES_KEY,
    CANCELLATION_REASON_KEY,
]

CATALOG_CANDIDATE_KEY = 'candidatos'
CATALOG_CANDIDATE_ASSETS_KEY = 'bens de candidatos'
# CATALOG_COALITIONS_KEY = 'coligacoes'
# CATALOG_VACANCIES_KEY = 'vagas'
CATALOG_CANCELLATION_REASON_KEY = 'motivo da cassacao'
CATALOG_KEYS = [
    CATALOG_CANDIDATE_KEY,
    CATALOG_CANDIDATE_ASSETS_KEY,
    # CATALOG_COALITIONS_KEY,
    # CATALOG_VACANCIES_KEY,
    CATALOG_CANCELLATION_REASON_KEY,
]
CATALOG_KEY_MAPPER = {
    CATALOG_CANDIDATE_KEY: CANDIDATE_KEY,
    CATALOG_CANDIDATE_ASSETS_KEY: CANDIDATE_ASSETS_KEY,
    # CATALOG_COALITIONS_KEY: COALITIONS_KEY,
    # CATALOG_VACANCIES_KEY: VACANCIES_KEY,
    CATALOG_CANCELLATION_REASON_KEY: CANCELLATION_REASON_KEY,
}


def _get_file_info(uri: str) -> Dict[str, Any]:
    with httpx.Client(timeout=30) as client:
        r = client.get(uri)
        r.raise_for_status()

    soup = BeautifulSoup(r.content, 'html.parser').find(id='content')

    file_uri = soup.find('section', role='complementary').select_one('p > a')['href']

    metadata = dict()
    file_date_info = dict()

    raw_metadata = soup.find('div', role='main').find('table').find('tbody').find_all('tr')
    for data in raw_metadata:
        key = data.find('th').text.strip()
        value = data.find('td').text.strip()

        if unidecode(key).lower() == 'criado':
            file_date_info[FILE_CREATED_AT_KEY] = dateparser.parse(value).date().isoformat()
        if unidecode(key).lower() == 'dados atualizados pela ultima vez':
            file_date_info[FILE_UPDATED_AT_KEY] = dateparser.parse(value).date().isoformat()

        metadata[key] = value

    info = {
        FILE_URI_KEY: file_uri,
        FILE_CREATED_AT_KEY: file_date_info.pop(FILE_CREATED_AT_KEY),
        FILE_UPDATED_AT_KEY: file_date_info.pop(FILE_UPDATED_AT_KEY),
        FILE_METADATA_KEY: metadata,
    }

    return info


def _get_catalog_files(uri: str) -> Dict[str, Dict[str, Any]]:
    with httpx.Client(timeout=30) as client:
        r = client.get(uri)
        r.raise_for_status()

    soup = BeautifulSoup(r.content, 'html.parser').find(id='content')
    catalog = soup.find(id='dataset-resources').find('ul').find_all('li', recursive=False)

    files = dict()

    for data in catalog:
        a = data.find('a')
        key = unidecode(a.next.strip()).lower()

        if key in CATALOG_KEYS:
            files[CATALOG_KEY_MAPPER[key]] = _get_file_info(uri=f"{BASE_URL}{a['href']}")

    return files


@task(multiple_outputs=False)
def candidatos() -> Dict[int, Dict[str, Dict[str, Any]]]:
    with httpx.Client(timeout=30) as client:
        r = client.get(CATALOG_URI)
        r.raise_for_status()

    soup = BeautifulSoup(r.content, 'html.parser').find(id='content')
    catalog = soup.find('ul', class_='dataset-list').find_all('li', recursive=False)

    links = dict()
    for data in catalog:
        link = data.find('div', class_='dataset-content').find('a')

        filetypes = [li.text.strip().upper() for li in data.find('ul').find_all('li')]
        if 'CSV' in filetypes:
            reference_year = int(link.text.strip().upper().split()[-1])

            links[reference_year] = _get_catalog_files(uri=f"{BASE_URL}{link['href']}")

    return links
