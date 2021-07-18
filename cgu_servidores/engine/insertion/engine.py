import csv
import re
from typing import Union
from datetime import datetime, date, time
from unidecode import unidecode
from pathlib import Path
from .register.employee import EmployeeInsertion
from .register.retired import RetiredInsertion
from .register.pensioner import PensionerInsertion
from ...models import InsertionTask, Remuneration, Fee, Observation, TimeOff


class Engine:
    def __init__(self, task: InsertionTask) -> None:
        self._task = task
        self._type = task.type
        self._release = task.release
        self._path = Path(task.file)

    def insert(self, buffer_size: int = 5000) -> int:
        insert_method = {
            InsertionTask.Type.EMPLOYEE: lambda n: EmployeeInsertion(self._path).insert(n),
            InsertionTask.Type.RETIRED: lambda n: RetiredInsertion(self._path).insert(n),
            InsertionTask.Type.PENSIONER: lambda n: PensionerInsertion(self._path).insert(n),
            InsertionTask.Type.FEE: self._insert_fees,
            InsertionTask.Type.REMUNERATION: self._insert_remuneration,
            InsertionTask.Type.OBSERVATION: self._insert_observations,
            InsertionTask.Type.TIMEOFF: self._insert_timeoffs,
        }[self._type]

        return insert_method(buffer_size)

    def _insert_remuneration(self, buffer_size: int) -> int:
        with self._path.open(mode='r', encoding='iso-8859-1') as f:
            reader = csv.DictReader((line.replace('\0', '') for line in f), delimiter=';')

            buffer = []
            counter = 0
            for line in reader:
                counter += 1

                try:
                    portal_id = int(line['Id_SERVIDOR_PORTAL'])
                except ValueError:
                    continue

                remuneration = dict(
                    reference_date=date(year=int(line['ANO']), month=int(line['MES']), day=1),
                    portal_id=portal_id,
                    cpf=re.sub(r'[^\d\*]', '', line['CPF']),
                    name=unidecode(line['NOME']).upper(),
                    base_remuneration_real=self._parse_money_value(line['REMUNERAÇÃO BÁSICA BRUTA (R$)']),
                    base_remuneration_dollar=self._parse_money_value(line['REMUNERAÇÃO BÁSICA BRUTA (U$)']),
                    ceiling_drop_real=self._parse_money_value(line['ABATE-TETO (R$)']),
                    ceiling_drop_dollar=self._parse_money_value(line['ABATE-TETO (U$)']),
                    christmas_bonus_real=self._parse_money_value(line['GRATIFICAÇÃO NATALINA (R$)']),
                    christmas_bonus_dollar=self._parse_money_value(line['GRATIFICAÇÃO NATALINA (U$)']),
                    ceiling_drop_christmas_bonus_real=self._parse_money_value(line['ABATE-TETO DA GRATIFICAÇÃO NATALINA (R$)']),
                    ceiling_drop_christmas_bonus_dollar=self._parse_money_value(line['ABATE-TETO DA GRATIFICAÇÃO NATALINA (U$)']),
                    vacation_real=self._parse_money_value(line['FÉRIAS (R$)']),
                    vacation_dollar=self._parse_money_value(line['FÉRIAS (U$)']),
                    eventual_real=self._parse_money_value(line['OUTRAS REMUNERAÇÕES EVENTUAIS (R$)']),
                    eventual_dollar=self._parse_money_value(line['OUTRAS REMUNERAÇÕES EVENTUAIS (U$)']),
                    irrf_real=self._parse_money_value(line['IRRF (R$)']),
                    irrf_dollar=self._parse_money_value(line['IRRF (U$)']),
                    social_welfare_real=self._parse_money_value(line['PSS/RPGS (R$)']),
                    social_welfare_dollar=self._parse_money_value(line['PSS/RPGS (U$)']),
                    other_deductions_real=self._parse_money_value(line['DEMAIS DEDUÇÕES (R$)']),
                    other_deductions_dollar=self._parse_money_value(line['DEMAIS DEDUÇÕES (U$)']),
                    military_pension_real=self._parse_money_value(line['PENSÃO MILITAR (R$)']),
                    military_pension_dollar=self._parse_money_value(line['PENSÃO MILITAR (U$)']),
                    health_fund_real=self._parse_money_value(line['FUNDO DE SAÚDE (R$)']),
                    health_fund_dollar=self._parse_money_value(line['FUNDO DE SAÚDE (U$)']),
                    property_cost_real=self._parse_money_value(line['TAXA DE OCUPAÇÃO IMÓVEL FUNCIONAL (R$)']),
                    property_cost_dollar=self._parse_money_value(line['TAXA DE OCUPAÇÃO IMÓVEL FUNCIONAL (U$)']),
                    remuneration_real=self._parse_money_value(line['REMUNERAÇÃO APÓS DEDUÇÕES OBRIGATÓRIAS (R$)']),
                    remuneration_dollar=self._parse_money_value(line['REMUNERAÇÃO APÓS DEDUÇÕES OBRIGATÓRIAS (U$)']),
                    indemnity_amount_civil_real=self._parse_money_value(
                        line['VERBAS INDENIZATÓRIAS REGISTRADAS EM SISTEMAS DE PESSOAL - CIVIL (R$)(*)']
                    ),
                    indemnity_amount_civil_dollar=self._parse_money_value(
                        line['VERBAS INDENIZATÓRIAS REGISTRADAS EM SISTEMAS DE PESSOAL - CIVIL (U$)(*)']
                    ),
                    indemnity_amount_militar_real=self._parse_money_value(
                        line['VERBAS INDENIZATÓRIAS REGISTRADAS EM SISTEMAS DE PESSOAL - MILITAR (R$)(*)']
                    ),
                    indemnity_amount_militar_dollar=self._parse_money_value(
                        line['VERBAS INDENIZATÓRIAS REGISTRADAS EM SISTEMAS DE PESSOAL - MILITAR (U$)(*)']
                    ),
                    indemnity_amount_mp_real=self._parse_money_value(
                        line['VERBAS INDENIZATÓRIAS PROGRAMA DESLIGAMENTO VOLUNTÁRIO  MP 792/2017 (R$)']
                    ),
                    indemnity_amount_mp_dollar=self._parse_money_value(
                        line['VERBAS INDENIZATÓRIAS PROGRAMA DESLIGAMENTO VOLUNTÁRIO  MP 792/2017 (U$)']
                    ),
                    indemnity_total_amount_real=self._parse_money_value(line['TOTAL DE VERBAS INDENIZATÓRIAS (R$)(*)']),
                    indemnity_total_amount_dollar=self._parse_money_value(line['TOTAL DE VERBAS INDENIZATÓRIAS (U$)(*)']),
                )
                buffer.append(remuneration)

                if len(buffer) >= buffer_size:
                    Remuneration.objects.bulk_upsert(conflict_target=['reference_date', 'portal_id'], rows=buffer)
                    buffer.clear()

            if buffer:
                Remuneration.objects.bulk_upsert(conflict_target=['reference_date', 'portal_id'], rows=buffer)

        return counter

    def _insert_fees(self, buffer_size: int) -> int:
        match_fields = ['type', 'reference_date', 'portal_id', 'cpf', 'name', 'company', 'observation', 'value']
        fee_type = Fee.Type.JETONS if self._path.name == 'jetons.csv' else Fee.Type.ATTORNEY

        with self._path.open(mode='r', encoding='iso-8859-1') as f:
            reader = csv.DictReader((line.replace('\0', '') for line in f), delimiter=';')

            buffer = {}
            late = {}
            counter = 0
            for line in reader:
                counter += 1

                try:
                    portal_id = int(line['Id_SERVIDOR_PORTAL'])
                except ValueError:
                    continue

                fee = dict(
                    type=fee_type,
                    reference_date=date(year=int(line['ANO']), month=int(line['MES']), day=1),
                    portal_id=portal_id,
                    cpf=re.sub(r'[^\d\*]', '', line['CPF']),
                    name=unidecode(line['NOME']).upper(),
                    company=line.get('EMPRESA', ''),
                    observation=line.get('OBSERVACOES', ''),
                    value=self._parse_money_value(line['VALOR']),
                )
                id = (
                    f"{fee['type']}{fee['reference_date']}{fee['portal_id']}{fee['cpf']}"
                    f"{fee['name']}{fee['company']}{fee['observation']}{fee['value']}"
                )
                if id in buffer:
                    late[id] = fee
                else:
                    buffer[id] = fee

                if len(buffer) >= buffer_size:
                    Fee.objects.bulk_upsert(conflict_target=match_fields, rows=buffer.values())
                    Fee.objects.bulk_upsert(conflict_target=match_fields, rows=late.values())
                    buffer.clear()
                    late.clear()

            if buffer:
                Fee.objects.bulk_upsert(conflict_target=match_fields, rows=buffer.values())

            if late:
                Fee.objects.bulk_upsert(conflict_target=match_fields, rows=late.values())

        return counter

    def _insert_observations(self, buffer_size: int) -> int:
        with self._path.open(mode='r', encoding='iso-8859-1') as f:
            reader = csv.DictReader((line.replace('\0', '') for line in f), delimiter=';')

            buffer = {}
            late = {}
            counter = 0
            for line in reader:
                counter += 1

                try:
                    portal_id = int(line['Id_SERVIDOR_PORTAL'])
                except ValueError:
                    continue

                observation = dict(
                    reference_date=date(year=int(line['ANO']), month=int(line['MES']), day=1),
                    portal_id=portal_id,
                    cpf=re.sub(r'[^\d\*]', '', line['CPF']),
                    name=unidecode(line['NOME']).upper(),
                    content=line['OBSERVACAO'],
                )
                id = f"{observation['reference_date']}{observation['portal_id']}{observation['content']}"
                if id in buffer:
                    late[id] = observation
                else:
                    buffer[id] = observation

                if len(buffer) >= buffer_size:
                    Observation.objects.bulk_upsert(conflict_target=['reference_date', 'portal_id', 'content'], rows=buffer.values())
                    Observation.objects.bulk_upsert(conflict_target=['reference_date', 'portal_id', 'content'], rows=late.values())
                    buffer.clear()
                    late.clear()

            if buffer:
                Observation.objects.bulk_upsert(conflict_target=['reference_date', 'portal_id', 'content'], rows=buffer.values())

            if late:
                Observation.objects.bulk_upsert(conflict_target=['reference_date', 'portal_id', 'content'], rows=late.values())

        return counter

    def _insert_timeoffs(self, buffer_size: int) -> int:
        with self._path.open(mode='r', encoding='iso-8859-1') as f:
            reader = csv.DictReader((line.replace('\0', '') for line in f), delimiter=';')

            buffer = {}
            late = {}
            counter = 0
            for line in reader:
                counter += 1

                try:
                    portal_id = int(line['Id_SERVIDOR_PORTAL'])
                except ValueError:
                    continue

                timeoff = dict(
                    reference_date=date(year=int(line['ANO']), month=int(line['MES']), day=1),
                    portal_id=portal_id,
                    cpf=re.sub(r'[^\d\*]', '', line['CPF']),
                    name=unidecode(line['NOME']).upper(),
                    start_date=self._parse_date(line['DATA_INICIO_AFASTAMENTO']),
                    end_date=self._parse_date(line['DATA_FIM_AFASTAMENTO']),
                )
                id = f"{timeoff['reference_date']}{timeoff['portal_id']}{timeoff['start_date']}{timeoff['end_date']}"
                if id in buffer:
                    late[id] = timeoff
                else:
                    buffer[id] = timeoff

                if len(buffer) >= buffer_size:
                    TimeOff.objects.bulk_upsert(conflict_target=['reference_date', 'portal_id', 'start_date', 'end_date'], rows=buffer.values())
                    TimeOff.objects.bulk_upsert(conflict_target=['reference_date', 'portal_id', 'start_date', 'end_date'], rows=late.values())
                    buffer.clear()
                    late.clear()

            if buffer:
                TimeOff.objects.bulk_upsert(conflict_target=['reference_date', 'portal_id', 'start_date', 'end_date'], rows=buffer.values())

            if late:
                TimeOff.objects.bulk_upsert(conflict_target=['reference_date', 'portal_id', 'start_date', 'end_date'], rows=late.values())

        return counter

    def _parse_money_value(self, value: str) -> int:
        if not re.match(r'^\-?(\.?\d{1,3})+,\d+$', value):
            return 0

        value = re.sub(r'[,\.]', '', value)
        value = int(value)
        return value

    def _parse_date(self, value: str) -> Union[date, None]:
        if re.match(r'^\d{2}/\d{2}/\d{4}$', value):
            return datetime.strptime(value, '%d/%m/%Y').date()

        return None
