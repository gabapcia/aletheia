import uuid
from django.db import models
from psqlextra.models import PostgresModel


class Release(PostgresModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    date = models.DateField(unique=True)
    finished = models.BooleanField(default=False)
    uri = models.TextField()
    folder = models.TextField()

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


class Person(PostgresModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    reference_date = models.DateField(help_text='Data de referência do registro')
    uf = models.TextField(help_text='Sigla da Unidade Federativa do beneficiário')
    county_code = models.TextField(help_text='Código, no IBGE, do município do beneficiário')
    county = models.TextField(help_text='Nome do município do beneficiário')
    nis = models.TextField(help_text='Número de NIS do beneficiário')
    cpf = models.TextField(help_text='Número no CPF do beneficiário')
    name = models.TextField(help_text='Nome do beneficiário')
    responsible_nis = models.TextField(help_text='Número de NIS do responsável pelo beneficiário')
    responsible_cpf = models.TextField(help_text='Número no CPF do responsável pelo beneficiário')
    responsible_name = models.TextField(help_text='Nome do responsável pelo beneficiário')
    framework = models.TextField(help_text='Identifica se o beneficiário faz parte de algum grupo de programas sociais')
    portion = models.IntegerField(help_text='Número sequencial da parcela disponibilizada')
    observation = models.TextField(help_text='Indica alterações na parcela disponibilizada como, por exemplo, se foi devolvida ou está retida')
    value = models.BigIntegerField(help_text='Valor disponibilizado na parcela')

    class Meta:
        indexes = [
            models.Index(fields=['created_at']),
            models.Index(fields=['updated_at']),
            models.Index(fields=['reference_date']),
            models.Index(fields=['uf']),
            models.Index(fields=['county_code']),
            models.Index(fields=['nis']),
            models.Index(fields=['cpf']),
            models.Index(fields=['name']),
            models.Index(fields=['responsible_nis']),
            models.Index(fields=['responsible_cpf']),
            models.Index(fields=['responsible_name']),
            models.Index(fields=['value']),
        ]
        constraints = [
            models.UniqueConstraint(
                name='unique_cgu_auxilioemergencial_person',
                fields=[
                    'reference_date',
                    'uf',
                    'county_code',
                    'nis',
                    'cpf',
                    'name',
                    'responsible_nis',
                    'responsible_cpf',
                    'responsible_name',
                    'framework',
                    'portion',
                    'observation',
                    'value',
                ],
            ),
        ]
