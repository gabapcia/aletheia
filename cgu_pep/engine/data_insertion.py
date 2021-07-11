import itertools
import re
from pathlib import Path
from csv import DictReader
from datetime import datetime
from unidecode import unidecode
from ..models import InsertionTask, Person


class Engine:
    PERSON_MATCH_FIELDS = [
        'cpf',
        'name',
        'role_initials',
        'role_description',
        'role_level',
        'federal_agency',
        'entry_date',
    ]

    def __init__(self, task: InsertionTask) -> None:
        self._filepath = Path(task.filepath)
        self._release = task.release
        self._start = task.start
        self._end = task.end

    def insert(self, batch_size: int = 5000) -> None:
        fieldnames = [
            'CPF',
            'Nome',
            'Sigla Funcao',
            'Descricao Funcao',
            'Nivel Funcao',
            'Nome Orgao',
            'Data Inicio Exercicio',
            'Data Fim Exercicio',
            'Data Fim Carencia',
        ]

        with self._filepath.open(mode='r', encoding='iso-8859-1') as f:
            f.readline()  # Skip header

            reader = DictReader(
                (line.replace('\0', '') for line in itertools.islice(f, self._start, self._end + 1)),
                fieldnames=fieldnames,
                delimiter=';',
            )

            buffer = []
            for line in reader:
                person = dict(
                    release=self._release,
                    cpf=re.sub(r'[^\d\*]', '', line['CPF']),
                    name=unidecode(line['Nome']).upper(),
                    role_initials=unidecode(line['Sigla Funcao']).upper(),
                    role_description=unidecode(line['Descricao Funcao']).upper(),
                    role_level=unidecode(line['Nivel Funcao']).upper(),
                    federal_agency=unidecode(line['Nome Orgao']).upper(),
                    entry_date=datetime.strptime(line['Data Inicio Exercicio'], '%d/%m/%Y').date(),
                    exit_date=(
                        datetime.strptime(line['Data Fim Exercicio'], '%d/%m/%Y').date()
                        if line['Data Fim Exercicio'] != 'Não informada' else None
                    ),
                    grace_period_end_date=(
                        datetime.strptime(line['Data Fim Carencia'], '%d/%m/%Y').date()
                        if line['Data Fim Carencia'] != 'Não informada' else None
                    ),
                )
                buffer.append(person)

                if len(buffer) >= batch_size:
                    self._insert(buffer)

            if buffer:
                self._insert(buffer)

    def _insert(self, data: list[dict]) -> None:
        Person.objects.bulk_upsert(conflict_target=Engine.PERSON_MATCH_FIELDS, rows=data)
        data.clear()
