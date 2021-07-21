import uuid
from django.db import models
from psqlextra.models import PostgresModel


class Release(PostgresModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    year = models.PositiveIntegerField(unique=True)
    finished = models.BooleanField(default=False)
    folder = models.TextField()
    uri = models.TextField()

    class Meta:
        indexes = [
            models.Index(fields=['created_at']),
            models.Index(fields=['updated_at']),
            models.Index(fields=['finished']),
        ]


class InsertionTask(PostgresModel):
    class Type(models.IntegerChoices):
        TRIP = 1
        TRIP_PART = 2
        TICKET = 3
        PAYMENT = 4

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    release = models.ForeignKey(Release, on_delete=models.CASCADE)

    finished = models.BooleanField(default=False)
    type = models.IntegerField(choices=Type.choices)
    start = models.PositiveBigIntegerField()
    end = models.PositiveBigIntegerField()
    filepath = models.TextField()

    class Meta:
        indexes = [
            models.Index(fields=['created_at']),
            models.Index(fields=['updated_at']),
            models.Index(fields=['finished']),
        ]


class Trip(PostgresModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    trip_id = models.TextField(help_text='Número que identifica o processo de concessão da viagem', unique=True)
    proposal_number = models.TextField(help_text='Número da Proposta (PCDP)')
    situation = models.TextField(help_text='Situação da viagem')
    urgent = models.BooleanField(help_text='É urgente?')
    urgency_justification = models.TextField(help_text='Justificativa da urgência')
    higher_agency_code = models.TextField(help_text='Código do Órgão Superior que custeou despesas da viagem')
    higher_agency = models.TextField(help_text='Nome do Órgão Superior')
    requesting_agency_code = models.TextField(help_text='Código do Órgão que solicitou a viagem')
    requesting_agency = models.TextField(help_text='Nome do Órgão')
    traveler_cpf = models.TextField(help_text='CPF da pessoa que realizou a viagem')
    traveler_name = models.TextField(help_text='Nome do viajante')
    traveler_position = models.TextField(help_text='Cargo do viajante')
    traveler_occupation = models.TextField(help_text='Função do viajante')
    traveler_occupation_description = models.TextField(help_text='Descrição da função do viajante')
    start_date = models.DateField(help_text='Data de início de afastamento do servidor')
    end_date = models.DateField(help_text='Data de fim de afastamento do servidor')
    destinations = models.TextField(help_text='Locais pelos quais o viajante passará durante a viagem')
    reason = models.TextField(help_text='Motivo da viagem')
    dailys_value = models.BigIntegerField(help_text='Valor de diárias pagas pelo órgão')
    tickets_value = models.BigIntegerField(help_text='Valor de passagens pagas pelo órgão')
    other_value = models.BigIntegerField(help_text='Valor de outros gastos pagos pelo órgão')

    class Meta:
        indexes = [
            models.Index(fields=['created_at']),
            models.Index(fields=['updated_at']),
            models.Index(fields=['proposal_number']),
            models.Index(fields=['urgent']),
            models.Index(fields=['higher_agency_code']),
            models.Index(fields=['requesting_agency_code']),
            models.Index(fields=['traveler_cpf']),
            models.Index(fields=['traveler_name']),
            models.Index(fields=['start_date']),
            models.Index(fields=['end_date']),
            models.Index(fields=['reason']),
            models.Index(fields=['dailys_value']),
            models.Index(fields=['tickets_value']),
            models.Index(fields=['other_value']),
        ]


class TripPart(PostgresModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    trip = models.ForeignKey(Trip, on_delete=models.CASCADE)

    proposal_number = models.TextField(help_text='')
    order = models.BigIntegerField(help_text='Sequência do trecho na viagem')
    origin_date = models.DateField(help_text='Data de saída da origem')
    country_origin = models.TextField(help_text='País de origem')
    uf_origin = models.TextField(help_text='UF de origem')
    city_origin = models.TextField(help_text='Cidade de origem')
    destiny_date = models.DateField(help_text='Data de chegada no destino')
    country_destiny = models.TextField(help_text='País de destino')
    uf_destiny = models.TextField(help_text='UF de destino')
    city_destiny = models.TextField(help_text='Cidade de destino')
    vehicle = models.TextField(help_text='Meio de transporte da origem para o destino')
    daily_value = models.BigIntegerField(help_text='Número de diárias do trecho')
    mission = models.BooleanField(help_text='Houve missão no trecho?')

    class Meta:
        indexes = [
            models.Index(fields=['created_at']),
            models.Index(fields=['updated_at']),
        ]
        constraints = [
            models.UniqueConstraint(
                name='unique_cgu_viagens_trip_part',
                fields=[
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
                ],
            ),
        ]


class Ticket(PostgresModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    trip = models.ForeignKey(Trip, on_delete=models.CASCADE)

    proposal_number = models.TextField(help_text='Número da Proposta (PCDP)')
    vehicle = models.TextField(help_text='Meio de transporte da passagem')
    outbound_country_origin = models.TextField(help_text='País de origem na ida')
    outbound_uf_origin = models.TextField(help_text='UF de origem na ida')
    outbound_city_origin = models.TextField(help_text='Cidade de origem na ida')
    outbound_country_destiny = models.TextField(help_text='País de destino na ida')
    outbound_uf_destiny = models.TextField(help_text='UF de destino na ida')
    outbound_city_destiny = models.TextField(help_text='Cidade de destino na ida')
    return_country_origin = models.TextField(help_text='País de origem na volta')
    return_uf_origin = models.TextField(help_text='UF de origem na volta')
    return_city_origin = models.TextField(help_text='Cidade de origem na volta')
    return_country_destiny = models.TextField(help_text='País de destino na volta')
    return_uf_destiny = models.TextField(help_text='UF de destino na volta')
    return_city_destiny = models.TextField(help_text='Cidade de destino na volta')
    value = models.BigIntegerField(help_text='Valor da passagem')
    service_charge = models.BigIntegerField(help_text='Taxa de serviço da passagem')
    buy_date = models.DateTimeField(null=True, help_text='Data de compra da passagem')

    class Meta:
        indexes = [
            models.Index(fields=['created_at']),
            models.Index(fields=['updated_at']),
        ]
        constraints = [
            models.UniqueConstraint(
                name='unique_cgu_viagens_ticket',
                fields=[
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
                ],
            ),
        ]


class Payment(PostgresModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    trip = models.ForeignKey(Trip, on_delete=models.CASCADE)

    proposal_number = models.TextField(help_text='Número da Proposta (PCDP)')
    higher_agency_code = models.TextField(help_text='Código do Órgão Superior que custeou a despesa')
    higher_agency = models.TextField(help_text='Nome do Órgão Superior')
    paying_agency_code = models.TextField(help_text='Código do Órgão que pagou a despesa')
    paying_agency = models.TextField(help_text='Nome do Órgão Pagador')
    management_unit_code = models.TextField(help_text='Código da Unidade Gestora que pagou a despesa')
    management_unit = models.TextField(help_text='Nome da Unidade Gestora')
    type = models.TextField(help_text='Tipo da despesa paga pelo órgão')
    value = models.BigIntegerField(help_text='Valor da despesa paga')

    class Meta:
        indexes = [
            models.Index(fields=['created_at']),
            models.Index(fields=['updated_at']),
            models.Index(fields=['type']),
            models.Index(fields=['value']),
        ]
        constraints = [
            models.UniqueConstraint(
                name='unique_cgu_viagens_payment',
                fields=[
                    'trip',
                    'proposal_number',
                    'higher_agency_code',
                    'paying_agency_code',
                    'management_unit_code',
                    'type',
                    'value',
                ],
            ),
        ]
