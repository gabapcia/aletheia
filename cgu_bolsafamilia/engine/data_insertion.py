import itertools
import re
from pathlib import Path
from csv import DictReader
from datetime import date, datetime
from typing import Any
from unidecode import unidecode
from ..models import InsertionTask, Payment, Withdraw


class Engine:
    def __init__(self, task: InsertionTask) -> None:
        self._filepath = Path(task.filepath)
        self._release = task.release
        self._type = task.type
        self._start = task.start
        self._end = task.end

    def insert(self, batch_size: int = 5000) -> None:
        insertion_func = {
            InsertionTask.Type.PAYMENT: self._insert_payments,
            InsertionTask.Type.WITHDRAW: self._insert_withdraws,
        }[self._type]

        insertion_func(batch_size=batch_size)

    def _insert_payments(self, batch_size: int) -> None:
        match_fields = [
            'reference_date',
            'competency_date',
            'federative_unit',
            'county_siafi_code',
            'recipient_cpf',
            'recipient_nis',
            'recipient_name',
            'value',
        ]

        def insert(data: dict[str, dict[str, Any]]) -> None:
            Payment.objects.bulk_upsert(conflict_target=match_fields, rows=data.values())
            data.clear()

        fieldnames = [
            'reference_date',
            'competency_date',
            'federative_unit',
            'county_siafi_code',
            'county',
            'recipient_cpf',
            'recipient_nis',
            'recipient_name',
            'value',
        ]

        with self._filepath.open(mode='r', encoding='iso-8859-1') as f:
            f.readline()  # Skip header

            reader = DictReader(
                (line.replace('\0', '') for line in itertools.islice(f, self._start, self._end + 1)),
                fieldnames=fieldnames,
                delimiter=';',
            )

            buffer = {}
            late = {}
            for line in reader:
                reference_date = date(
                    year=int(line['reference_date'][:4]),
                    month=int(line['reference_date'][4:]),
                    day=1,
                )
                competency_date = date(
                    year=int(line['competency_date'][:4]),
                    month=int(line['competency_date'][4:]),
                    day=1,
                )

                payment = dict(
                    release=self._release,
                    reference_date=reference_date,
                    competency_date=competency_date,
                    federative_unit=unidecode(line['federative_unit']).upper(),
                    county_siafi_code=line['county_siafi_code'],
                    county=unidecode(line['county']).upper(),
                    recipient_cpf=re.sub(r'[^\d\*]', '', line['recipient_cpf']),
                    recipient_nis=line['recipient_nis'],
                    recipient_name=unidecode(line['recipient_name']).upper(),
                    value=int(re.sub(r'\D', '', line['value'])),
                )
                id = ''.join([
                    payment['reference_date'].isoformat(),
                    payment['competency_date'].isoformat(),
                    payment['federative_unit'],
                    payment['county_siafi_code'],
                    payment['recipient_cpf'],
                    payment['recipient_nis'],
                    payment['recipient_name'],
                    str(payment['value']),
                ])

                if not buffer.get(id):
                    buffer[id] = payment
                else:
                    late[id] = payment

                if len(buffer) >= batch_size:
                    insert(buffer)
                    insert(late)

            if buffer:
                insert(buffer)
            if late:
                insert(late)

    def _insert_withdraws(self, batch_size: int) -> None:
        match_fields = [
            'date',
            'reference_date',
            'competency_date',
            'federative_unit',
            'county_siafi_code',
            'recipient_cpf',
            'recipient_nis',
            'recipient_name',
            'value',
        ]

        def insert(data: dict[str, dict[str, Any]]) -> None:
            Withdraw.objects.bulk_upsert(conflict_target=match_fields, rows=data.values())
            data.clear()

        fieldnames = [
            'reference_date',
            'competency_date',
            'federative_unit',
            'county_siafi_code',
            'county',
            'recipient_cpf',
            'recipient_nis',
            'recipient_name',
            'date',
            'value',
        ]

        with self._filepath.open(mode='r', encoding='iso-8859-1') as f:
            f.readline()  # Skip header

            reader = DictReader(
                (line.replace('\0', '') for line in itertools.islice(f, self._start, self._end + 1)),
                fieldnames=fieldnames,
                delimiter=';',
            )

            buffer = {}
            late = {}
            for line in reader:
                reference_date = date(
                    year=int(line['reference_date'][:4]),
                    month=int(line['reference_date'][4:]),
                    day=1,
                )
                competency_date = date(
                    year=int(line['competency_date'][:4]),
                    month=int(line['competency_date'][4:]),
                    day=1,
                )

                payment = dict(
                    release=self._release,
                    reference_date=reference_date,
                    competency_date=competency_date,
                    federative_unit=unidecode(line['federative_unit']).upper(),
                    county_siafi_code=line['county_siafi_code'],
                    county=unidecode(line['county']).upper(),
                    recipient_cpf=re.sub(r'[^\d\*]', '', line['recipient_cpf']),
                    recipient_nis=line['recipient_nis'],
                    recipient_name=unidecode(line['recipient_name']).upper(),
                    date=datetime.strptime(line['date'], '%d/%m/%Y').date(),
                    value=int(re.sub(r'\D', '', line['value'])),
                )
                id = ''.join([
                    payment['reference_date'].isoformat(),
                    payment['competency_date'].isoformat(),
                    payment['date'].isoformat(),
                    payment['federative_unit'],
                    payment['county_siafi_code'],
                    payment['recipient_cpf'],
                    payment['recipient_nis'],
                    payment['recipient_name'],
                    str(payment['value']),
                ])

                if not buffer.get(id):
                    buffer[id] = payment
                else:
                    late[id] = payment

                if len(buffer) >= batch_size:
                    insert(buffer)
                    insert(late)

            if buffer:
                insert(buffer)
            if late:
                insert(late)
