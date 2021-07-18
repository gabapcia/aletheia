import csv
from typing import Union
from datetime import datetime, date
from unidecode import unidecode
import re
from pathlib import Path
from ....models import Pensioner


class PensionerInsertion:
    def __init__(self, path: Path) -> None:
        self._path = path

    def insert(self, buffer_size: int) -> None:
        match_fields = [
            'portal_id',
            'relationship_code',
            'relationship_status',
            'legal_representative_cpf',
            'legal_representative_name',
            'pension_type_code',
            'pension_start_date',
            'pi_cpf',
            'pi_name',
            'pi_organizational_unit_located_code',
            'pi_agency_office_code',
            'pi_higher_agency_office_code',
            'pi_legal_regime',
            'pi_workload',
            'pi_position_admission_date',
            'pi_position_appointment_date',
            'pi_agency_admission_date',
            'pi_admission_degree_date',
            'pi_public_service_admission_degree',
        ]

        with self._path.open(mode='r', encoding='iso-8859-1') as f:
            reader = csv.DictReader((line.replace('\0', '') for line in f), delimiter=';')

            buffer = {}
            late = {}
            counter = 0
            for line in reader:
                counter += 1
                pensioner = dict(
                    portal_id=int(line['Id_SERVIDOR_PORTAL']),
                    cpf=re.sub(r'[^\d\*]', '', line['CPF']),
                    name=unidecode(line['NOME']).upper(),
                    register=line['MATRICULA'],
                    relationship_code=line['COD_TIPO_VINCULO'],
                    relationship=line['TIPO_VINCULO'],
                    relationship_status=line['SITUACAO_VINCULO'],
                    legal_representative_cpf=re.sub(r'[^\d\*]', '', line['CPF_REPRESENTANTE_LEGAL']),
                    legal_representative_name=unidecode(line['NOME_REPRESENTANTE_LEGAL']).upper(),
                    pension_type_code=line['COD_TIPO_PENSAO'],
                    pension_type=line['TIPO_PENSAO'],
                    pension_start_date=self._parse_date(line['DATA_INICIO_PENSAO']),
                    pi_cpf=re.sub(r'[^\d\*]', '', line['CPF_INSTITUIDOR_PENSAO']),
                    pi_name=unidecode(line['NOME_INSTITUIDOR_PENSAO']).upper(),
                    pi_position=line['DESCRICAO_CARGO_INSTITUIDOR_PENSAO'],
                    pi_organizational_unit_located_code=line['COD_UORG_LOTACAO_INSTITUIDOR_PENSAO'],
                    pi_organizational_unit_located=line['UORG_LOTACAO_INSTITUIDOR_PENSAO'],
                    pi_agency_office_code=line['COD_ORG_LOTACAO_INSTITUIDOR_PENSAO'],
                    pi_agency_office=line['ORG_LOTACAO_INSTITUIDOR_PENSAO'],
                    pi_higher_agency_office_code=line['COD_ORGSUP_LOTACAO_INSTITUIDOR_PENSAO'],
                    pi_higher_agency_office=line['ORGSUP_LOTACAO_INSTITUIDOR_PENSAO'],
                    pi_legal_regime=line['REGIME_JURIDICO_INSTITUIDOR_PENSAO'],
                    pi_workload=line['JORNADA_DE_TRABALHO_INSTITUIDOR_PENSAO'],
                    pi_position_admission_date=self._parse_date(line['DATA_INGRESSO_CARGOFUNCAO_INSTITUIDOR_PENSAO']),
                    pi_position_appointment_date=self._parse_date(line['DATA_NOMEACAO_CARGOFUNCAO_INSTITUIDOR_PENSAO']),
                    pi_agency_admission_date=self._parse_date(line['DATA_INGRESSO_ORGAO_INSTITUIDOR_PENSAO']),
                    pi_admission_document=line['DOCUMENTO_INGRESSO_SERVICOPUBLICO_INSTITUIDOR_PENSAO'],
                    pi_admission_degree_date=self._parse_date(line['DATA_DIPLOMA_INGRESSO_SERVICOPUBLICO_INSTITUIDOR_PENSAO']),
                    pi_position_admission_degree=line['DIPLOMA_INGRESSO_CARGOFUNCAO_INSTITUIDOR_PENSAO'],
                    pi_agency_admission_degree=line['DIPLOMA_INGRESSO_ORGAO_INSTITUIDOR_PENSAO'],
                    pi_public_service_admission_degree=line['DIPLOMA_INGRESSO_SERVICOPUBLICO_INSTITUIDOR_PENSAO'],
                )
                id = ''.join(map(lambda f: str(pensioner[f]), match_fields))
                if id in buffer:
                    late[id] = pensioner
                else:
                    buffer[id] = pensioner

                if len(buffer) >= buffer_size:
                    Pensioner.objects.bulk_upsert(conflict_target=match_fields, rows=buffer.values())
                    Pensioner.objects.bulk_upsert(conflict_target=match_fields, rows=late.values())
                    buffer.clear()
                    late.clear()

            if buffer:
                Pensioner.objects.bulk_upsert(conflict_target=match_fields, rows=buffer.values())

            if late:
                Pensioner.objects.bulk_upsert(conflict_target=match_fields, rows=late.values())

        return counter

    def _parse_date(self, value: str) -> Union[date, None]:
        if re.match(r'^\d{2}/\d{2}/\d{4}$', value):
            return datetime.strptime(value, '%d/%m/%Y').date()

        return None
