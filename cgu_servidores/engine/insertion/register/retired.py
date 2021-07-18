import csv
from typing import Union
from datetime import datetime, date
from unidecode import unidecode
import re
from pathlib import Path
from ....models import Retired


class RetiredInsertion:
    def __init__(self, path: Path) -> None:
        self._path = path

    def insert(self, buffer_size: int) -> None:
        match_fields = [
            'portal_id',
            'relationship_code',
            'relationship_status',
            'retirement_type_code',
            'retirement_date',
            'organizational_unit_located_code',
            'agency_located_code',
            'higher_agency_located_code',
            'legal_regime',
            'workload',
            'position_admission_date',
            'position_appointment_date',
            'agency_admission_date',
            'admission_degree_date',
            'public_service_admission_degree',
        ]

        with self._path.open(mode='r', encoding='iso-8859-1') as f:
            reader = csv.DictReader((line.replace('\0', '') for line in f), delimiter=';')

            buffer = {}
            late = {}
            counter = 0
            for line in reader:
                counter += 1
                retired = dict(
                    portal_id=int(line['Id_SERVIDOR_PORTAL']),
                    cpf=re.sub(r'[^\d\*]', '', line['CPF']),
                    name=unidecode(line['NOME']).upper(),
                    register=line['MATRICULA'],
                    relationship_code=line['COD_TIPO_VINCULO'],
                    relationship=line['TIPO_VINCULO'],
                    relationship_status=line['SITUACAO_VINCULO'],
                    retirement_type_code=line['COD_TIPO_APOSENTADORIA'],
                    retirement_type=line['TIPO_APOSENTADORIA'],
                    retirement_date=self._parse_date(line['DATA_APOSENTADORIA']),
                    position_description=line['DESCRICAO_CARGO'],
                    organizational_unit_located_code=line['COD_UORG_LOTACAO'],
                    organizational_unit_located=line['UORG_LOTACAO'],
                    agency_located_code=line['COD_ORG_LOTACAO'],
                    agency_located=line['ORG_LOTACAO'],
                    higher_agency_located_code=line['COD_ORGSUP_LOTACAO'],
                    higher_agency_located=line['ORGSUP_LOTACAO'],
                    legal_regime=line['REGIME_JURIDICO'],
                    workload=line['JORNADA_DE_TRABALHO'],
                    position_admission_date=self._parse_date(line['DATA_INGRESSO_CARGOFUNCAO']),
                    position_appointment_date=self._parse_date(line['DATA_NOMEACAO_CARGOFUNCAO']),
                    agency_admission_date=self._parse_date(line['DATA_INGRESSO_ORGAO']),
                    admission_document=line['DOCUMENTO_INGRESSO_SERVICOPUBLICO'],
                    admission_degree_date=self._parse_date(line['DATA_DIPLOMA_INGRESSO_SERVICOPUBLICO']),
                    position_admission_degree=line['DIPLOMA_INGRESSO_CARGOFUNCAO'],
                    agency_admission_degree=line['DIPLOMA_INGRESSO_ORGAO'],
                    public_service_admission_degree=line['DIPLOMA_INGRESSO_SERVICOPUBLICO'],
                )
                id = ''.join(map(lambda f: str(retired[f]), match_fields))
                if id in buffer:
                    late[id] = retired
                else:
                    buffer[id] = retired

                if len(buffer) >= buffer_size:
                    Retired.objects.bulk_upsert(conflict_target=match_fields, rows=buffer.values())
                    Retired.objects.bulk_upsert(conflict_target=match_fields, rows=late.values())
                    buffer.clear()
                    late.clear()

            if buffer:
                Retired.objects.bulk_upsert(conflict_target=match_fields, rows=buffer.values())

            if late:
                Retired.objects.bulk_upsert(conflict_target=match_fields, rows=late.values())

        return counter

    def _parse_date(self, value: str) -> Union[date, None]:
        if re.match(r'^\d{2}/\d{2}/\d{4}$', value):
            return datetime.strptime(value, '%d/%m/%Y').date()

        return None
