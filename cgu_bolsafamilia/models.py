import uuid
from django.db import models
from psqlextra.models import PostgresModel


class Release(PostgresModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    finished = models.BooleanField(default=False)

    date = models.DateField(unique=True)
    folder = models.TextField()
    payment_uri = models.URLField()
    withdraw_uri = models.URLField()

    class Meta:
        indexes = [
            models.Index(fields=['created_at']),
            models.Index(fields=['updated_at']),
            models.Index(fields=['finished']),
        ]


class InsertionTask(PostgresModel):
    class Type(models.IntegerChoices):
        PAYMENT = 1
        WITHDRAW = 2

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    release = models.ForeignKey(Release, on_delete=models.CASCADE)

    finished = models.BooleanField(default=False)
    type = models.IntegerField(choices=Type.choices)
    filepath = models.TextField()
    start = models.PositiveBigIntegerField()
    end = models.PositiveBigIntegerField()

    class Meta:
        indexes = [
            models.Index(fields=['created_at']),
            models.Index(fields=['updated_at']),
            models.Index(fields=['type']),
            models.Index(fields=['finished']),
        ]


class Payment(PostgresModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    release = models.ForeignKey(Release, on_delete=models.CASCADE)

    reference_date = models.DateField(help_text='Data de referência')
    competency_date = models.DateField(help_text='Data de competência')
    federative_unit = models.CharField(max_length=2, help_text='Unidade Federativa (UF)')
    county_siafi_code = models.CharField(max_length=5, help_text='Código SIAFI do município')
    county = models.CharField(max_length=512, help_text='Nome do município')
    recipient_cpf = models.CharField(max_length=11, help_text='CPF do favorecido')
    recipient_nis = models.CharField(max_length=11, help_text='NIS do favorecido')
    recipient_name = models.CharField(max_length=512, help_text='Nome do favorecido')
    value = models.BigIntegerField(help_text='Valor da parcela')

    class Meta:
        indexes = [
            models.Index(fields=['created_at']),
            models.Index(fields=['updated_at']),
            models.Index(fields=['reference_date']),
            models.Index(fields=['competency_date']),
            models.Index(fields=['federative_unit']),
            models.Index(fields=['county_siafi_code']),
            models.Index(fields=['recipient_cpf']),
            models.Index(fields=['recipient_nis']),
            models.Index(fields=['recipient_name']),
            models.Index(fields=['value']),
        ]
        constraints = [
            models.UniqueConstraint(
                name='unique_payment_entry',
                fields=[
                    'reference_date',
                    'competency_date',
                    'federative_unit',
                    'county_siafi_code',
                    'recipient_cpf',
                    'recipient_nis',
                    'recipient_name',
                    'value',
                ],
            ),
        ]


class Withdraw(PostgresModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    release = models.ForeignKey(Release, on_delete=models.CASCADE)

    date = models.DateField(help_text='Data do saque')
    reference_date = models.DateField(help_text='Data de referência')
    competency_date = models.DateField(help_text='Data de competência')
    federative_unit = models.CharField(max_length=2, help_text='Unidade Federativa (UF)')
    county_siafi_code = models.CharField(max_length=5, help_text='Código SIAFI do município')
    county = models.CharField(max_length=512, help_text='Nome do município')
    recipient_cpf = models.CharField(max_length=11, help_text='CPF do favorecido')
    recipient_nis = models.CharField(max_length=11, help_text='NIS do favorecido')
    recipient_name = models.CharField(max_length=512, help_text='Nome do favorecido')
    value = models.BigIntegerField(help_text='Valor da parcela')

    class Meta:
        indexes = [
            models.Index(fields=['created_at']),
            models.Index(fields=['updated_at']),
            models.Index(fields=['date']),
            models.Index(fields=['reference_date']),
            models.Index(fields=['competency_date']),
            models.Index(fields=['federative_unit']),
            models.Index(fields=['county_siafi_code']),
            models.Index(fields=['recipient_cpf']),
            models.Index(fields=['recipient_nis']),
            models.Index(fields=['recipient_name']),
            models.Index(fields=['value']),
        ]
        constraints = [
            models.UniqueConstraint(
                name='unique_withdraw_entry',
                fields=[
                    'date',
                    'reference_date',
                    'competency_date',
                    'federative_unit',
                    'county_siafi_code',
                    'recipient_cpf',
                    'recipient_nis',
                    'recipient_name',
                    'value',
                ],
            ),
        ]
