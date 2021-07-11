import re
from django.db.models.query import QuerySet
import django_filters as filters
from .models import Person


class PersonFilter(filters.FilterSet):
    name = filters.CharFilter(method='filter_name', label='Nome do PEP')
    cpf = filters.CharFilter(method='filter_cpf', label='CPF/CNPJ do PEP')

    def filter_partner_name(self, queryset: QuerySet, name: str, value: str) -> QuerySet:
        return queryset.filter(name=value.upper())

    def filter_partner_doc(self, queryset: QuerySet, name: str, value: str) -> QuerySet:
        tax_id = re.sub(r'\D', '', value)
        tax_id = f'***{tax_id[3:9]}**'

        return queryset.filter(cpf=tax_id)

    class Meta:
        model = Person
        fields = []
