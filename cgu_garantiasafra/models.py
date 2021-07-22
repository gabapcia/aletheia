import uuid
from django.db import models
from psqlextra.models import PostgresModel


class Release(PostgresModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    date = models.DateField(unique=True)
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
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    release = models.ForeignKey(Release, on_delete=models.CASCADE)

    finished = models.BooleanField(default=False)
    start = models.PositiveBigIntegerField()
    end = models.PositiveBigIntegerField()
    filepath = models.TextField()

    class Meta:
        indexes = [
            models.Index(fields=['created_at']),
            models.Index(fields=['updated_at']),
            models.Index(fields=['finished']),
        ]


class Warranty(PostgresModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    reference_date = models.DateField(help_text='Ano/Mês da folha de pagamento')
    uf = models.TextField(help_text='Sigla da Unidade Federativa do beneficiário do Garantia Safra')
    county_siafi_code = models.TextField(help_text='Código, no SIAFI, do município do beneficiário do Garantia Safra')
    county = models.TextField(help_text='Nome do município do beneficiário do Garantia Safra')
    recipient_nis = models.TextField(help_text='NIS do beneficiário do Garantia Safra')
    recipient_name = models.TextField(help_text='Nome do beneficiário do Garantia Safra')
    value = models.BigIntegerField(help_text='Valor da parcela do benefício')

    class Meta:
        indexes = [
            models.Index(fields=['created_at']),
            models.Index(fields=['updated_at']),
            models.Index(fields=['reference_date']),
            models.Index(fields=['uf']),
            models.Index(fields=['county_siafi_code']),
            models.Index(fields=['recipient_nis']),
            models.Index(fields=['recipient_name']),
            models.Index(fields=['value']),
        ]
        constraints = [
            models.UniqueConstraint(
                name='unique_cgu_garantiasafra_warranty',
                fields=[
                    'reference_date',
                    'uf',
                    'county_siafi_code',
                    'recipient_nis',
                    'recipient_name',
                    'value',
                ],
            ),
        ]
