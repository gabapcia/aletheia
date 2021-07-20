import itertools
import re
from pathlib import Path
from csv import DictReader
from datetime import datetime
from typing import Any
from unidecode import unidecode
from ..models import InsertionTask, Person


class Engine:
    PERSON_MATCH_FIELDS = [
        'reference_date',
        'uf',
        'county_code',
        'nis',
        'cpf',
        'name',
        'responsible_nis',
        'responsible_cpf',
        'responsible_name',
        'framework',
        'portion',
        'observation',
        'value',
    ]

    def __init__(self, task: InsertionTask) -> None:
        self._filepath = Path(task.filepath)
        self._release = task.release
        self._start = task.start
        self._end = task.end

    def insert(self, batch_size: int = 5000) -> None:
        with self._filepath.open(mode='r', encoding='iso-8859-1') as f:
            fieldnames = unidecode(f.readline().strip().replace('"', '').upper()).split(';')
            reader = DictReader(
                (line.replace('\0', '') for line in itertools.islice(f, self._start, self._end + 1)),
                fieldnames=fieldnames,
                delimiter=';',
            )

            buffer = {}
            late = {}
            for line in reader:
                responsible_name = unidecode(line['NOME RESPONSAVEL']).upper()
                if responsible_name == 'NAO SE APLICA':
                    responsible_name = ''

                person = dict(
                    reference_date=datetime.strptime(line['MES DISPONIBILIZACAO'], '%Y%m').date(),
                    uf=line['UF'],
                    county_code=line['CODIGO MUNICIPIO IBGE'],
                    county=unidecode(line['NOME MUNICIPIO']).upper(),
                    nis=line['NIS BENEFICIARIO'],
                    cpf=re.sub(r'[^\d\*]', '', line['CPF BENEFICIARIO']),
                    name=line['NOME BENEFICIARIO'],
                    responsible_nis=line['NIS RESPONSAVEL'] if responsible_name else '',
                    responsible_cpf=re.sub(r'[^\d\*]', '', line['CPF RESPONSAVEL']) if responsible_name else '',
                    responsible_name=responsible_name,
                    framework=line['ENQUADRAMENTO'],
                    portion=int(re.sub('ª', '', line['PARCELA'])),
                    observation=line['OBSERVACAO'] if unidecode(line['OBSERVACAO']).upper() != 'NAO HA' else '',
                    value=int(re.sub(r'\D', '', line['VALOR BENEFICIO'])),
                )
                id = ''.join((str(person[field]) for field in Engine.PERSON_MATCH_FIELDS))
                if id in buffer:
                    late[id] = person
                else: 
                    buffer[id] = person

                if len(buffer) >= batch_size:
                    self._insert(buffer)
                    if late:
                        self._insert(late)

            if buffer:
                self._insert(buffer)

            if late:
                self._insert(late)

    def _insert(self, data: dict[str, dict[str, Any]]) -> None:
        Person.objects.bulk_upsert(conflict_target=Engine.PERSON_MATCH_FIELDS, rows=data.values())
        data.clear()
