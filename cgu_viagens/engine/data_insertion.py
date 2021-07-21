import itertools
import logging
import re
from pathlib import Path
from csv import DictReader
from datetime import datetime
from unidecode import unidecode
from ..models import InsertionTask, Trip, TripPart, Ticket, Payment


class Engine:
    def __init__(self, task: InsertionTask) -> None:
        if task.type != InsertionTask.Type.TRIP:
            pending_trip_tasks = task.release.insertiontask_set.filter(type=InsertionTask.Type.TRIP, finished=False)
            assert not pending_trip_tasks.exists(), 'Trips not inserted'

        self._filepath = Path(task.filepath)
        self._release = task.release
        self._type = task.type
        self._start = task.start
        self._end = task.end

    def insert(self, batch_size: int = 5000) -> None:
        insertion_func = {
            InsertionTask.Type.TRIP: self._insert_trips,
            InsertionTask.Type.TRIP_PART: self._insert_trip_parts,
            InsertionTask.Type.TICKET: self._insert_tickets,
            InsertionTask.Type.PAYMENT: self._insert_payments,
        }[self._type]

        return insertion_func(batch_size)

    def _insert_payments(self, batch_size: int) -> None:
        match_fields = [
            'trip',
            'proposal_number',
            'higher_agency_code',
            'paying_agency_code',
            'management_unit_code',
            'type',
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
                trip_id = line['Identificador do processo de viagem']

                try:
                    trip = Trip.objects.get(trip_id=trip_id)
                except Trip.DoesNotExist:
                    logging.warning(f'Trip with ID "{trip_id}" not found')
                    continue

                payment = dict(
                    trip=trip,
                    proposal_number=line['Número da Proposta (PCDP)'],
                    higher_agency_code=line['Código do órgão superior'],
                    higher_agency=line['Nome do órgão superior'],
                    paying_agency_code=line['Codigo do órgão pagador'],
                    paying_agency=line['Nome do órgao pagador'],
                    management_unit_code=line['Código da unidade gestora pagadora'],
                    management_unit=line['Nome da unidade gestora pagadora'],
                    type=line['Tipo de pagamento'],
                    value=int(re.sub(r'\D', '', line['Valor'])),
                )
                id = ''.join((str(payment[field]) for field in match_fields))
                if id in buffer:
                    late[id] = payment
                else:
                    buffer[id] = payment

                if len(buffer) >= batch_size:
                    Payment.objects.bulk_upsert(conflict_target=match_fields, rows=buffer.values())
                    buffer.clear()
                    if late:
                        Payment.objects.bulk_upsert(conflict_target=match_fields, rows=late.values())
                        late.clear()

            if buffer:
                Payment.objects.bulk_upsert(conflict_target=match_fields, rows=buffer.values())

            if late:
                Payment.objects.bulk_upsert(conflict_target=match_fields, rows=late.values())

    def _insert_tickets(self, batch_size: int) -> None:
        match_fields = [
            'trip',
            'proposal_number',
            'vehicle',
            'outbound_country_origin',
            'outbound_uf_origin',
            'outbound_city_origin',
            'outbound_country_destiny',
            'outbound_uf_destiny',
            'outbound_city_destiny',
            'return_country_origin',
            'return_uf_origin',
            'return_city_origin',
            'return_country_destiny',
            'return_uf_destiny',
            'return_city_destiny',
            'value',
            'service_charge',
            'buy_date',
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
                trip_id = line['Identificador do processo de viagem']

                try:
                    trip = Trip.objects.get(trip_id=trip_id)
                except Trip.DoesNotExist:
                    logging.warning(f'Trip with ID "{trip_id}" not found')
                    continue

                ticket = dict(
                    trip=trip,
                    proposal_number=line['Número da Proposta (PCDP)'],
                    vehicle=line['Meio de transporte'],
                    outbound_country_origin=self._clean_text(line['País - Origem ida']),
                    outbound_uf_origin=self._clean_text(line['UF - Origem ida']),
                    outbound_city_origin=self._clean_text(line['Cidade - Origem ida']),
                    outbound_country_destiny=self._clean_text(line['País - Destino ida']),
                    outbound_uf_destiny=self._clean_text(line['UF - Destino ida']),
                    outbound_city_destiny=self._clean_text(line['Cidade - Destino ida']),
                    return_country_origin=self._clean_text(line['País - Origem volta']),
                    return_uf_origin=self._clean_text(line['UF - Origem volta']),
                    return_city_origin=self._clean_text(line['Cidade - Origem volta']),
                    return_country_destiny=self._clean_text(line['Pais - Destino volta']),
                    return_uf_destiny=self._clean_text(line['UF - Destino volta']),
                    return_city_destiny=self._clean_text(line['Cidade - Destino volta']),
                    value=int(re.sub(r'\D', '', line['Valor da passagem'])),
                    service_charge=int(re.sub(r'\D', '', line['Taxa de serviço'])),
                    buy_date=(
                        datetime.strptime(f"{line['Data da emissão/compra']} {line['Hora da emissão/compra']}", '%d/%m/%Y %H:%M')
                        if self._clean_text(line['Data da emissão/compra']) else None
                    ),
                )
                id = ''.join((str(ticket[field]) for field in match_fields))
                if id in buffer:
                    late[id] = ticket
                else:
                    buffer[id] = ticket

                if len(buffer) >= batch_size:
                    Ticket.objects.bulk_upsert(conflict_target=match_fields, rows=buffer.values())
                    buffer.clear()
                    if late:
                        Ticket.objects.bulk_upsert(conflict_target=match_fields, rows=late.values())
                        late.clear()

            if buffer:
                Ticket.objects.bulk_upsert(conflict_target=match_fields, rows=buffer.values())

            if late:
                Ticket.objects.bulk_upsert(conflict_target=match_fields, rows=late.values())

    def _insert_trip_parts(self, batch_size: int) -> None:
        match_fields = [
            'trip',
            'proposal_number',
            'order',
            'origin_date',
            'country_origin',
            'uf_origin',
            'city_origin',
            'destiny_date',
            'country_destiny',
            'uf_destiny',
            'city_destiny',
            'vehicle',
            'daily_value',
            'mission',
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
                trip_id = line['Identificador do processo de viagem ']

                try:
                    trip = Trip.objects.get(trip_id=trip_id)
                except Trip.DoesNotExist:
                    logging.warning(f'Trip with ID "{trip_id}" not found')
                    continue

                trip_part = dict(
                    trip=trip,
                    proposal_number=line['Número da Proposta (PCDP)'],
                    order=int(line['Sequência Trecho']),
                    origin_date=datetime.strptime(line['Origem - Data'], '%d/%m/%Y').date(),
                    country_origin=line['Origem - País'],
                    uf_origin=line['Origem - UF'],
                    city_origin=line['Origem - Cidade'],
                    destiny_date=datetime.strptime(line['Destino - Data'], '%d/%m/%Y'),
                    country_destiny=line['Destino - País'],
                    uf_destiny=line['Destino - UF'],
                    city_destiny=line['Destino - Cidade'],
                    vehicle=line['Meio de transporte'],
                    daily_value=int(re.sub(r'\D', '', line['Número Diárias'])),
                    mission=unidecode(line['Missao?']).upper() == 'SIM',
                )
                id = ''.join((str(trip_part[field]) for field in match_fields))
                if id in buffer:
                    late[id] = trip_part
                else:
                    buffer[id] = trip_part

                if len(buffer) >= batch_size:
                    TripPart.objects.bulk_upsert(conflict_target=match_fields, rows=buffer.values())
                    buffer.clear()
                    if late:
                        TripPart.objects.bulk_upsert(conflict_target=match_fields, rows=late.values())
                        late.clear()

            if buffer:
                TripPart.objects.bulk_upsert(conflict_target=match_fields, rows=buffer.values())

            if late:
                TripPart.objects.bulk_upsert(conflict_target=match_fields, rows=late.values())

    def _insert_trips(self, batch_size: int) -> None:
        match_fields = ['trip_id']

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
                trip = dict(
                    trip_id=line['Identificador do processo de viagem'],
                    proposal_number=line['Número da Proposta (PCDP)'],
                    situation=line['Situação'],
                    urgent=unidecode(line['Viagem Urgente']).upper() == 'SIM',
                    urgency_justification=line['Justificativa Urgência Viagem'],
                    higher_agency_code=line['Código do órgão superior'],
                    higher_agency=line['Nome do órgão superior'],
                    requesting_agency_code=line['Código órgão solicitante'],
                    requesting_agency=line['Nome órgão solicitante'],
                    traveler_cpf=re.sub(r'[^\d\*]', '', line['CPF viajante']),
                    traveler_name=unidecode(line['Nome']).upper(),
                    traveler_position=line['Cargo'],
                    traveler_occupation=line['Função'],
                    traveler_occupation_description=line['Descrição Função'],
                    start_date=datetime.strptime(line['Período - Data de início'], '%d/%m/%Y').date(),
                    end_date=datetime.strptime(line['Período - Data de fim'], '%d/%m/%Y').date(),
                    destinations=line['Destinos'],
                    reason=line['Motivo'],
                    dailys_value=int(re.sub(r'\D', '', line['Valor diárias'])),
                    tickets_value=int(re.sub(r'\D', '', line['Valor passagens'])),
                    other_value=int(re.sub(r'\D', '', line['Valor outros gastos'])),
                )
                id = ''.join((str(trip[field]) for field in match_fields))
                if id in buffer:
                    late[id] = trip
                else:
                    buffer[id] = trip

                if len(buffer) >= batch_size:
                    Trip.objects.bulk_upsert(conflict_target=match_fields, rows=buffer.values())
                    buffer.clear()
                    if late:
                        Trip.objects.bulk_upsert(conflict_target=match_fields, rows=late.values())
                        late.clear()

            if buffer:
                Trip.objects.bulk_upsert(conflict_target=match_fields, rows=buffer.values())

            if late:
                Trip.objects.bulk_upsert(conflict_target=match_fields, rows=late.values())

    def _clean_text(self, value: str) -> str:
        if unidecode(value).upper() == 'SEM INFORMACAO':
            return ''

        return value
