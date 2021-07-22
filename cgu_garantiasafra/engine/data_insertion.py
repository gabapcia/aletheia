import itertools
import re
from pathlib import Path
from csv import DictReader
from datetime import datetime
from unidecode import unidecode
from ..models import InsertionTask, Warranty


class Engine:
    def __init__(self, task: InsertionTask) -> None:
        self._filepath = Path(task.filepath)
        self._release = task.release
        self._start = task.start
        self._end = task.end

    def insert(self, batch_size: int = 5000) -> None:
        match_fields = [
            'reference_date',
            'uf',
            'county_siafi_code',
            'recipient_nis',
            'recipient_name',
            'value',
        ]

        with self._filepath.open(mode='r', encoding='iso-8859-1') as f:
            fieldnames = f.readline().strip().replace('"', '').split(';')

            reader = DictReader(
                (line.replace('\0', '') for line in itertools.islice(f, self._start, self._end + 1)),
                fieldnames=fieldnames,
                delimiter=';',
            )

            buffer = {}
            late = {}
            for line in reader:
                warranty = dict(
                    reference_date=datetime.strptime(line['MÊS REFERÊNCIA'], '%Y%m').date(),
                    uf=line['UF'],
                    county_siafi_code=line['CÓDIGO MUNICÍPIO SIAFI'],
                    county=line['NOME MUNICÍPIO'],
                    recipient_nis=line['NIS FAVORECIDO'],
                    recipient_name=unidecode(line['NOME FAVORECIDO']).upper(),
                    value=int(re.sub(r'[^\d\-]', '', line['VALOR PARCELA'])),
                )
                id = ''.join((str(warranty[field]) for field in match_fields))
                if id in buffer:
                    late[id] = warranty
                else:
                    buffer[id] = warranty

                if len(buffer) >= batch_size:
                    Warranty.objects.bulk_upsert(conflict_target=match_fields, rows=buffer.values())
                    buffer.clear()
                    if late:
                        Warranty.objects.bulk_upsert(conflict_target=match_fields, rows=late.values())
                        late.clear()

            if buffer:
                Warranty.objects.bulk_upsert(conflict_target=match_fields, rows=buffer.values())

            if late:
                Warranty.objects.bulk_upsert(conflict_target=match_fields, rows=late.values())
