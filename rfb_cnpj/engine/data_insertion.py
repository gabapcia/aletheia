import gc
import itertools
import re
from pathlib import Path
from csv import DictReader
from datetime import datetime
from unidecode import unidecode
from .schema.legal_nature import LEGAL_NATURE
from .schema.qualification import QUALIFICATION
from .schema.reason import REASON
from .schema.country import COUNTRY
from .schema.county import COUNTY
from .schema.age_group import AGE_GROUP
from ..models import InsertionTask, Company, Establishment, Simples, Partner
from ..exceptions import CompaniesNotInserted


class Engine:
    def __init__(self, task: InsertionTask) -> None:
        if task.type != InsertionTask.Type.COMPANIES:
            pending_company_tasks = InsertionTask.objects.filter(
                release=task.release,
                type=InsertionTask.Type.COMPANIES,
                finished=False,
            ).exists()
            if pending_company_tasks:
                raise CompaniesNotInserted

        self._filepath = Path(task.filepath)
        self._type = task.type
        self._release = task.release
        self._start = task.start
        self._end = task.end

    def insert(self, batch_size: int = 5000) -> None:
        insert_func = {
            InsertionTask.Type.COMPANIES: self._insert_companies,
            InsertionTask.Type.SIMPLES: self._insert_simples,
            InsertionTask.Type.ESTABLISHMENTS: self._insert_establishments,
            InsertionTask.Type.PARTNERS: self._insert_partners,
        }[self._type]

        insert_func(batch_size=batch_size)

    def _insert_companies(self, batch_size: int) -> None:
        def insert(data: dict[str, list[dict]]) -> None:
            Company.objects.bulk_upsert(conflict_target=['base_cnpj'], rows=data.values())
            data.clear()
            gc.collect()

        fieldnames = [
            'base_cnpj',
            'name',
            'legal_nature',
            'qualification_person_responsible',
            'share_capital',
            'size',
            'responsible_federative_entity',
        ]

        with self._filepath.open(mode='r', encoding='iso-8859-1') as f:
            reader = DictReader(
                (line.replace('\0', '') for line in itertools.islice(f, self._start, self._end + 1)),
                fieldnames=fieldnames,
                delimiter=';',
            )

            late = {}
            buffer = {}
            for line in reader:
                company = dict(
                    release=self._release,
                    base_cnpj=line['base_cnpj'],
                    name=unidecode(line['name']).upper(),
                    legal_nature=LEGAL_NATURE.get(line['legal_nature']) or LEGAL_NATURE['0000'],
                    qualification_person_responsible=QUALIFICATION.get(line['qualification_person_responsible']) or QUALIFICATION['00'],
                    share_capital=int(line['share_capital'].replace(',', '')),
                    size=int(line['size'] or 1),
                    responsible_federative_entity=line['responsible_federative_entity'],
                )
                if line['base_cnpj'] in buffer:
                    late[line['base_cnpj']] = company
                else:
                    buffer[line['base_cnpj']] = company

                if len(buffer) >= batch_size:
                    insert(buffer)
                    if late:
                        insert(late)

            if buffer:
                insert(buffer)
            if late:
                insert(late)

    def _insert_simples(self, batch_size: int) -> None:
        def insert(data: dict[str, list[dict]]) -> None:
            Simples.objects.bulk_upsert(conflict_target=['base_cnpj'], rows=data.values())
            data.clear()
            gc.collect()

        fieldnames = [
            'base_cnpj',
            'simples_option',
            'simples_option_date',
            'simples_exclusion_date',
            'mei_option',
            'mei_option_date',
            'mei_exclusion_date',
        ]

        with self._filepath.open(mode='r', encoding='iso-8859-1') as f:
            reader = DictReader(
                (line.replace('\0', '') for line in itertools.islice(f, self._start, self._end + 1)),
                fieldnames=fieldnames,
                delimiter=';',
            )

            late = {}
            buffer = {}
            for line in reader:
                simples_option = {
                    '': 3,
                    'S': 1,
                    'N': 2,
                }[line['simples_option'].strip().upper()]
                mei_option = {
                    '': 3,
                    'S': 1,
                    'N': 2,
                }[line['mei_option'].strip().upper()]

                try:
                    company = Company.objects.get(base_cnpj=line['base_cnpj'])
                except Company.DoesNotExist:
                    print(f"Missing base CNPJ: {line['base_cnpj']}")
                    continue

                simples = dict(
                    company=company,
                    base_cnpj=line['base_cnpj'],
                    simples_option=simples_option,
                    simples_option_date=(
                        datetime.strptime(line['simples_option_date'], '%Y%m%d').date()
                        if line['simples_option_date'] and line['simples_option_date'] != '00000000'
                        else None
                    ),
                    simples_exclusion_date=(
                        datetime.strptime(line['simples_exclusion_date'], '%Y%m%d').date()
                        if line['simples_exclusion_date'] and line['simples_exclusion_date'] != '00000000'
                        else None
                    ),
                    mei_option=mei_option,
                    mei_option_date=(
                        datetime.strptime(line['mei_option_date'], '%Y%m%d').date()
                        if line['mei_option_date'] and line['mei_option_date'] != '00000000'
                        else None
                    ),
                    mei_exclusion_date=(
                        datetime.strptime(line['mei_exclusion_date'], '%Y%m%d').date()
                        if line['mei_exclusion_date'] and line['mei_exclusion_date'] != '00000000'
                        else None
                    ),
                )
                if line['base_cnpj'] in buffer:
                    late[line['base_cnpj']] = simples
                else:
                    buffer[line['base_cnpj']] = simples

                if len(buffer) >= batch_size:
                    insert(buffer)
                    if late:
                        insert(late)

            if buffer:
                insert(buffer)
            if late:
                insert(late)

    def _insert_establishments(self, batch_size: int) -> None:
        def insert(data: dict[str, list[dict]]) -> None:
            Establishment.objects.bulk_upsert(conflict_target=['cnpj'], rows=data.values())
            data.clear()
            gc.collect()

        fieldnames = [
            'cnpj_base',
            'cnpj_order',
            'cnpj_dv',
            'identification',
            'fantasy_name',
            'registration_status',
            'registration_status_date',
            'registration_status_reason',
            'city_name_abroad',
            'country',
            'activity_start_date',
            'main_cnae',
            'secondary_cnaes',
            'street_type',
            'place',
            'number',
            'complement',
            'district',
            'zip_code',
            'federative_unit',
            'county',
            'ddd_phone_number_1',
            'phone_number_1',
            'ddd_phone_number_2',
            'phone_number_2',
            'ddd_fax_number',
            'fax_number',
            'email',
            'special_situation',
            'date_special_situation',
        ]

        with self._filepath.open(mode='r', encoding='iso-8859-1') as f:
            reader = DictReader(
                (line.replace('\0', '') for line in itertools.islice(f, self._start, self._end + 1)),
                fieldnames=fieldnames,
                delimiter=';',
            )

            late = {}
            buffer = {}
            for line in reader:
                cnpj = line['cnpj_base'] + line['cnpj_order'] + line['cnpj_dv']
                establishment = dict(
                    release=self._release,
                    company=Company.objects.get(base_cnpj=line['cnpj_base']),
                    cnpj=cnpj,
                    identification=int(line['identification']),
                    fantasy_name=unidecode(line['fantasy_name']).upper(),
                    registration_status=int(line['registration_status']),
                    registration_status_date=(
                        datetime.strptime(line['registration_status_date'], '%Y%m%d').date()
                        if line['registration_status_date'] and not re.search(r'^0+$', line['registration_status_date'])
                        else None
                    ),
                    registration_status_reason=REASON.get(line['registration_status_reason']) or REASON['00'],
                    city_name_abroad=line['city_name_abroad'],
                    country=COUNTRY.get(line['country'], ''),
                    activity_start_date=(
                        datetime.strptime(line['activity_start_date'], '%Y%m%d').date()
                        if line['activity_start_date'] and not re.search(r'^0+$', line['activity_start_date'])
                        else None
                    ),
                    main_cnae=line['main_cnae'],
                    street_type=line['street_type'],
                    place=line['place'],
                    number=line['number'],
                    complement=line['complement'],
                    district=line['district'],
                    zip_code=line['zip_code'],
                    federative_unit=line['federative_unit'],
                    county=COUNTY[line['county']],
                    phone_number_1=line['ddd_phone_number_1'] + line['phone_number_1'],
                    phone_number_2=line['ddd_phone_number_2'] + line['phone_number_2'],
                    fax_number=line['ddd_fax_number'] + line['fax_number'],
                    email=line['email'].upper(),
                    special_situation=line['special_situation'],
                    special_situation_date=(
                        datetime.strptime(line['date_special_situation'], '%Y%m%d').date()
                        if line['date_special_situation'] and re.search(r'^0+$', line['date_special_situation'])
                        else None
                    ),
                )
                if cnpj in buffer:
                    late[cnpj] = establishment
                else:
                    buffer[cnpj] = establishment

                if len(buffer) >= batch_size:
                    insert(buffer)
                    if late:
                        insert(late)

            if buffer:
                insert(buffer)
            if late:
                insert(late)

    def _insert_partners(self, batch_size: int) -> None:
        match_fields = ['company', 'identifier', 'name', 'cpf_cnpj', 'join_date']

        def insert(data: dict[str, list[dict]]) -> None:
            Partner.objects.bulk_upsert(conflict_target=match_fields, rows=data.values())
            data.clear()
            gc.collect()

        fieldnames = [
            'base_cnpj',
            'identifier',
            'name',
            'cpf_cnpj',
            'qualification',
            'join_date',
            'country',
            'legal_representative',
            'legal_representative_name',
            'legal_representative_qualification',
            'age_group',
        ]

        with self._filepath.open(mode='r', encoding='iso-8859-1') as f:
            reader = DictReader(
                (line.replace('\0', '') for line in itertools.islice(f, self._start, self._end + 1)),
                fieldnames=fieldnames,
                delimiter=';',
            )

            late = {}
            buffer = {}
            for line in reader:
                legal_representative = line['legal_representative'] if line['legal_representative'] != '***000000**' else ''

                partner = dict(
                    release=self._release,
                    company=Company.objects.get(base_cnpj=line['base_cnpj']),
                    identifier=int(line['identifier']),
                    name=unidecode(line['name']).upper(),
                    cpf_cnpj=line['cpf_cnpj'],
                    qualification=QUALIFICATION.get(line['qualification']) or QUALIFICATION['00'],
                    join_date=datetime.strptime(line['join_date'], '%Y%m%d').date(),
                    country=COUNTRY.get(line['country'], ''),
                    legal_representative=legal_representative,
                    legal_representative_name=line['legal_representative_name'],
                    legal_representative_qualification=QUALIFICATION[line['legal_representative_qualification']] if legal_representative else '',
                    age_group=AGE_GROUP[line['age_group']],
                )
                id = f"{partner['company']}{partner['identifier']}{partner['name']}{partner['cpf_cnpj']}{partner['join_date'].isoformat()}"
                if id in buffer:
                    late[id] = partner
                else:
                    buffer[id] = partner

                if len(buffer) >= batch_size:
                    insert(buffer)
                    if late:
                        insert(late)

            if buffer:
                insert(buffer)
            if late:
                insert(late)
