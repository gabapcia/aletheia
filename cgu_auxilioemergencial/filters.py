import re
from django.db.models.query import QuerySet
import django_filters as filters
from .models import Person


class PersonFilter(filters.FilterSet):
    name = filters.CharFilter(method='filter_name', label='Nome do beneficiário')
    cpf = filters.CharFilter(method='filter_cpf', label='CPF do beneficiário')
    nis = filters.CharFilter(label='NIS do beneficiário')
    responsible_name = filters.CharFilter(method='filter_responsible_cpf', label='CPF do responsável pelo beneficiário')
    responsible_cpf = filters.CharFilter(method='filter_responsible_name', label='Nome do responsável pelo beneficiário')
    responsible_nis = filters.CharFilter(label='NIS do responsável pelo beneficiário')
    uf = filters.CharFilter(label='UF do beneficiário')
    county_code = filters.CharFilter(label='Código IBGE do município do beneficiário')
    value = filters.NumberFilter(label='Valor da parcela')

    def filter_name(self, queryset: QuerySet, name: str, value: str) -> QuerySet:
        return queryset.filter(name=value.upper())

    def filter_cpf(self, queryset: QuerySet, name: str, value: str) -> QuerySet:
        tax_id = re.sub(r'\D', '', value)
        tax_id = f'***{tax_id[3:9]}**'

        return queryset.filter(cpf=tax_id)

    def filter_responsible_name(self, queryset: QuerySet, name: str, value: str) -> QuerySet:
        return queryset.filter(responsible_name=value.upper())

    def filter_responsible_cpf(self, queryset: QuerySet, name: str, value: str) -> QuerySet:
        tax_id = re.sub(r'\D', '', value)
        tax_id = f'***{tax_id[3:9]}**'

        return queryset.filter(responsible_cpf=tax_id)

    class Meta:
        model = Person
        fields = []
