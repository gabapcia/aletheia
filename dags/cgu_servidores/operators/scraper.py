import json
import re
from datetime import date
from typing import Dict, List, Union
import httpx
from bs4 import BeautifulSoup
from unidecode import unidecode
from airflow.decorators import task


BASE_URL = 'https://www.portaltransparencia.gov.br/download-de-dados/servidores'

FILEDATE_KEY = 'date'
RETIRED_KEY = 'retired'
PENSIONER_KEY = 'pensioner'
EMPLOYEE_KEY = 'employee'
HONORARY_KEY = 'honorary'
FILE_TYPES = [RETIRED_KEY, PENSIONER_KEY, EMPLOYEE_KEY, HONORARY_KEY]
RETIRED_BACEN_FILENAME = 'Aposentados_BACEN'
RETIRED_SIAPE_FILENAME = 'Aposentados_SIAPE'
RETIRED_MILITARY_FILENAME = 'Reserva_Reforma_Militares'
PENSIONER_BACEN_FILENAME = 'Pensionistas_BACEN'
PENSIONER_SIAPE_FILENAME = 'Pensionistas_SIAPE'
PENSIONER_DEFESA_FILENAME = 'Pensionistas_DEFESA'
EMPLOYEE_BACEN_FILENAME = 'Servidores_BACEN'
EMPLOYEE_SIAPE_FILENAME = 'Servidores_SIAPE'
EMPLOYEE_MILITARY_FILENAME = 'Militares'
HONORARY_ADVOCATIVE_FILENAME = 'Honorarios_Advocaticios'
HONORARY_JETONS_FILENAME = 'Honorarios_Jetons'
RETIRED_FILE_NAMES = [RETIRED_BACEN_FILENAME, RETIRED_SIAPE_FILENAME, RETIRED_MILITARY_FILENAME]
PENSIONER_FILE_NAMES = [PENSIONER_BACEN_FILENAME, PENSIONER_SIAPE_FILENAME, PENSIONER_DEFESA_FILENAME]
EMPLOYEE_FILE_NAMES = [EMPLOYEE_BACEN_FILENAME, EMPLOYEE_SIAPE_FILENAME, EMPLOYEE_MILITARY_FILENAME]
HONORARY_FILE_NAMES = [HONORARY_ADVOCATIVE_FILENAME, HONORARY_JETONS_FILENAME]
ALL_FILE_NAMES = RETIRED_FILE_NAMES + PENSIONER_FILE_NAMES + EMPLOYEE_FILE_NAMES + HONORARY_FILE_NAMES


class MissingFileMap(Exception):
    def __init__(self, file_types: List[str]) -> None:
        self.missing = set(file_types) - set(ALL_FILE_NAMES)

    def __str__(self) -> str:
        files = ', '.join(f'"{m}"' for m in self.missing)
        return f'Missing mapper for files with origin: {files}'


def _group_files(data: Dict[str, str]) -> Dict[str, Dict[str, str]]:
    report = {
        RETIRED_KEY: {},
        PENSIONER_KEY: {},
        EMPLOYEE_KEY: {},
        HONORARY_KEY: {},
    }

    for filename in RETIRED_FILE_NAMES:
        report[RETIRED_KEY][filename] = data.get(filename, '')

    for filename in PENSIONER_FILE_NAMES:
        report[PENSIONER_KEY][filename] = data.get(filename, '')

    for filename in EMPLOYEE_FILE_NAMES:
        report[EMPLOYEE_KEY][filename] = data.get(filename, '')

    for filename in HONORARY_FILE_NAMES:
        report[HONORARY_KEY][filename] = data.get(filename, '')

    return report


@task(multiple_outputs=False)
def servidores() -> List[Dict[str, Union[str, Dict[str, Dict[str, str]]]]]:
    with httpx.Client(timeout=5) as client:
        r = client.get(BASE_URL)
        r.raise_for_status()

    soup = BeautifulSoup(r.content, 'html.parser')

    for script in soup.find_all('script'):
        text = unidecode(str(script))
        if 'var arquivos =' in text:
            break

    raw_links = dict()
    for match in re.finditer(r'arquivos\.push\((?P<data>[^\)]+)\)', text):
        data = json.loads(match.group('data'))

        source = data['origem']
        file_date = date(year=int(data['ano']), month=int(data['mes']), day=1).isoformat()
        file_uri = f"{BASE_URL}/{data['ano']}{data['mes']}_{source}"

        if file_date not in raw_links:
            raw_links[file_date] = dict()

        raw_links[file_date][source] = file_uri

    links = list()
    for file_date, file_types in raw_links.items():
        if len(file_types.keys()) > len(ALL_FILE_NAMES):
            raise MissingFileMap(file_types=list(file_types.keys()))

        links.append({
            **_group_files(file_types),
            FILEDATE_KEY: file_date,
        })

    return links
