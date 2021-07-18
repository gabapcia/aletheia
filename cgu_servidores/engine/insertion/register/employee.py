import csv
from datetime import date, datetime
from typing import Union
from unidecode import unidecode
import re
from pathlib import Path
from ....models import Employee


class EmployeeInsertion:
    def __init__(self, path: Path) -> None:
        self._path = path

    def insert(self, buffer_size: int) -> int:
        match_fields = [
            'portal_id',
            'uf',
            'relationship_code',
            'relationship_status',
            'position_tier',
            'position_default',
            'position_level',
            'occupation_initials',
            'activity_code',
            'partial_option',
            'organizational_unit_located_code',
            'agency_located_code',
            'higher_agency_located_code',
            'organizational_unit_office_code',
            'agency_office_code',
            'higher_agency_office_code',
            'timeoff_start_date',
            'timeoff_end_date',
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
                employee = dict(
                    portal_id=int(line['Id_SERVIDOR_PORTAL']),
                    cpf=re.sub(r'[^\d\*]', '', line['CPF']),
                    name=unidecode(line['NOME']).upper(),
                    register=line['MATRICULA'],
                    uf=line['UF_EXERCICIO'],
                    relationship_code=line['COD_TIPO_VINCULO'],
                    relationship=line['TIPO_VINCULO'],
                    relationship_status=line['SITUACAO_VINCULO'],
                    position_description=line['DESCRICAO_CARGO'],
                    position_tier=line['CLASSE_CARGO'],
                    position_reference=line['REFERENCIA_CARGO'],
                    position_default=line['PADRAO_CARGO'],
                    position_level=line['NIVEL_CARGO'],
                    occupation_initials=line['SIGLA_FUNCAO'],
                    occupation_level=line['NIVEL_FUNCAO'],
                    occupation=line['FUNCAO'],
                    activity_code=line['CODIGO_ATIVIDADE'],
                    activity=line['ATIVIDADE'],
                    partial_option=line['OPCAO_PARCIAL'],
                    organizational_unit_located_code=line['COD_UORG_LOTACAO'],
                    organizational_unit_located=line['UORG_LOTACAO'],
                    agency_located_code=line['COD_ORG_LOTACAO'],
                    agency_located=line['ORG_LOTACAO'],
                    higher_agency_located_code=line['COD_ORGSUP_LOTACAO'],
                    higher_agency_located=line['ORGSUP_LOTACAO'],
                    organizational_unit_office_code=line['COD_UORG_EXERCICIO'],
                    organizational_unit_office=line['UORG_EXERCICIO'],
                    agency_office_code=line['COD_ORG_EXERCICIO'],
                    agency_office=line['ORG_EXERCICIO'],
                    higher_agency_office_code=line['COD_ORGSUP_EXERCICIO'],
                    higher_agency_office=line['ORGSUP_EXERCICIO'],
                    timeoff_start_date=self._parse_date(line['DATA_INICIO_AFASTAMENTO']),
                    timeoff_end_date=self._parse_date(line['DATA_TERMINO_AFASTAMENTO']),
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
                id = ''.join(map(lambda f: str(employee[f]), match_fields))
                if id in buffer:
                    late[id] = employee
                else:
                    buffer[id] = employee

                if len(buffer) >= buffer_size:
                    Employee.objects.bulk_upsert(conflict_target=match_fields, rows=buffer.values())
                    Employee.objects.bulk_upsert(conflict_target=match_fields, rows=late.values())
                    buffer.clear()
                    late.clear()

            if buffer:
                Employee.objects.bulk_upsert(conflict_target=match_fields, rows=buffer.values())

            if late:
                Employee.objects.bulk_upsert(conflict_target=match_fields, rows=late.values())

        return counter

    def _parse_date(self, value: str) -> Union[date, None]:
        if re.match(r'^\d{2}/\d{2}/\d{4}$', value):
            return datetime.strptime(value, '%d/%m/%Y').date()

        return None
