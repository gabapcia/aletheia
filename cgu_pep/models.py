import uuid
from django.db import models
from django.db.models import Q
from psqlextra.models import PostgresModel


class Release(PostgresModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    date = models.DateField(unique=True)
    finished = models.BooleanField(default=False)
    folder = models.TextField()

    class Meta:
        indexes = [
            models.Index(fields=['created_at']),
            models.Index(fields=['updated_at']),
            models.Index(fields=['finished']),
        ]
        constraints = [
            models.UniqueConstraint(
                name='unique_pendind_cgu_pep_release',
                fields=['finished'],
                condition=Q(finished=False),
            ),
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
            models.Index(fields=['finished']),
        ]


class Person(PostgresModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    release = models.ForeignKey(Release, on_delete=models.CASCADE)

    cpf = models.CharField(max_length=11, help_text='CPF')
    name = models.CharField(max_length=512, help_text='Nome')
    role_initials = models.CharField(max_length=512, help_text='Sigla da função')
    role_description = models.CharField(max_length=512, help_text='Descrição da função')
    role_level = models.CharField(max_length=512, help_text='Nível da função')
    federal_agency = models.CharField(max_length=512, help_text='Nome do órgão')
    entry_date = models.DateField(help_text='Data de início das atividades')
    exit_date = models.DateField(null=True, help_text='Data de término das atividades')
    grace_period_end_date = models.DateField(null=True, help_text='Data de término do período de carência')

    class Meta:
        indexes = [
            models.Index(fields=['cpf']),
            models.Index(fields=['name']),
        ]
        constraints = [
            models.UniqueConstraint(
                name='unique_person_pep',
                fields=[
                    'cpf',
                    'name',
                    'role_initials',
                    'role_description',
                    'role_level',
                    'federal_agency',
                    'entry_date',
                ],
            ),
        ]
