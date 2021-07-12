import re
from django.db.models.query import QuerySet
import django_filters as filters
from .models import Payment, Withdraw


class PaymentFilter(filters.FilterSet):
    name = filters.CharFilter(method='filter_name', label='Nome do favorecido')
    cpf = filters.CharFilter(method='filter_cpf', label='CPF do favorecido')
    nis = filters.CharFilter(field_name='recipient_nis', help_text='NIS do favorecido')
    reference_date = filters.DateFilter(help_text='Data de referência')
    competency_date = filters.DateFilter(help_text='Data de competência')
    federative_unit = filters.CharFilter(help_text='Unidade Federativa (UF)')
    county_siafi_code = filters.CharFilter(help_text='Código SIAFI do município')
    value = filters.NumberFilter(help_text='Valor da parcela')

    def filter_name(self, queryset: QuerySet, name: str, value: str) -> QuerySet:
        return queryset.filter(name=value.upper())

    def filter_cpf(self, queryset: QuerySet, name: str, value: str) -> QuerySet:
        tax_id = re.sub(r'\D', '', value)
        tax_id = f'***{tax_id[3:9]}**'

        return queryset.filter(cpf=tax_id)

    class Meta:
        model = Payment
        fields = []


class WithdrawFilter(filters.FilterSet):
    name = filters.CharFilter(method='filter_name', label='Nome do favorecido')
    cpf = filters.CharFilter(method='filter_cpf', label='CPF do favorecido')
    nis = filters.CharFilter(field_name='recipient_nis', label='NIS do favorecido')
    date = filters.CharFilter(label='Data do saque')
    reference_date = filters.DateFilter(label='Data de referência')
    competency_date = filters.DateFilter(label='Data de competência')
    federative_unit = filters.CharFilter(label='Unidade Federativa (UF)')
    county_siafi_code = filters.CharFilter(label='Código SIAFI do município')
    value = filters.NumberFilter(label='Valor da parcela')

    def filter_name(self, queryset: QuerySet, name: str, value: str) -> QuerySet:
        return queryset.filter(name=value.upper())

    def filter_cpf(self, queryset: QuerySet, name: str, value: str) -> QuerySet:
        tax_id = re.sub(r'\D', '', value)
        tax_id = f'***{tax_id[3:9]}**'

        return queryset.filter(cpf=tax_id)

    class Meta:
        model = Withdraw
        fields = []
