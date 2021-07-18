import re
from django.db.models.query import QuerySet
import django_filters as filters
from .models import Employee, Retired, Pensioner


class EmployeeFilter(filters.FilterSet):
    portal_id = filters.NumberFilter(label='ID do servidor no portal da CGU')
    name = filters.CharFilter(method='filter_name', label='Nome do servidor')
    cpf = filters.CharFilter(method='filter_cpf', label='CPF do servidor')

    def filter_name(self, queryset: QuerySet, name: str, value: str) -> QuerySet:
        return queryset.filter(name=value.upper())

    def filter_cpf(self, queryset: QuerySet, name: str, value: str) -> QuerySet:
        tax_id = re.sub(r'\D', '', value)
        tax_id = f'***{tax_id[3:9]}**'

        return queryset.filter(cpf=tax_id)

    class Meta:
        model = Employee
        fields = []


class RetiredFilter(EmployeeFilter):
    portal_id = filters.NumberFilter(label='ID do aposentado no portal da CGU')
    name = filters.CharFilter(method='filter_name', label='Nome do aposentado')
    cpf = filters.CharFilter(method='filter_cpf', label='CPF do aposentado')

    def filter_name(self, queryset: QuerySet, name: str, value: str) -> QuerySet:
        return queryset.filter(name=value.upper())

    def filter_cpf(self, queryset: QuerySet, name: str, value: str) -> QuerySet:
        tax_id = re.sub(r'\D', '', value)
        tax_id = f'***{tax_id[3:9]}**'

        return queryset.filter(cpf=tax_id)

    class Meta:
        model = Retired
        fields = []


class PensionerFilter(EmployeeFilter):
    portal_id = filters.NumberFilter(label='ID do pensionista no portal da CGU')
    name = filters.CharFilter(method='filter_name', label='Nome do pensionista')
    cpf = filters.CharFilter(method='filter_cpf', label='CPF do pensionista')

    def filter_name(self, queryset: QuerySet, name: str, value: str) -> QuerySet:
        return queryset.filter(name=value.upper())

    def filter_cpf(self, queryset: QuerySet, name: str, value: str) -> QuerySet:
        tax_id = re.sub(r'\D', '', value)
        tax_id = f'***{tax_id[3:9]}**'

        return queryset.filter(cpf=tax_id)

    class Meta:
        model = Pensioner
        fields = []
