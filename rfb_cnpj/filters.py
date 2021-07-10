import re
from django.db.models.query import QuerySet
from django.db.models import Q
import django_filters as filters
from .models import Establishment


class EstablishmentFilter(filters.FilterSet):
    cnpj = filters.CharFilter(label='CNPJ')
    base_cnpj = filters.CharFilter(field_name='company', lookup_expr='base_cnpj', label='CNPJ Base')
    name = filters.CharFilter(method='filter_name', label='Razão social')
    fantasy_name = filters.CharFilter(method='filter_fantasy_name', label='Nome fantasia')
    email = filters.CharFilter(method='filter_email', label='E-mail')
    phone_number = filters.CharFilter(method='filter_phone_number', label='Número de telefone')
    fax_number = filters.CharFilter(label='Número do FAX')
    partner_name = filters.CharFilter(method='filter_partner_name', label='Nome do sócio')
    partner_doc = filters.CharFilter(method='filter_partner_doc', label='CPF/CNPJ do sócio')

    def filter_name(self, queryset: QuerySet, name: str, value: str) -> QuerySet:
        return queryset.filter(company__name=value.upper())

    def filter_fantasy_name(self, queryset: QuerySet, name: str, value: str) -> QuerySet:
        return queryset.filter(fantasy_name=value.upper())

    def filter_email(self, queryset: QuerySet, name: str, value: str) -> QuerySet:
        return queryset.filter(email=value.lower())

    def filter_phone_number(self, queryset: QuerySet, name: str, value: str) -> QuerySet:
        return queryset.filter(Q(phone_number_1=value) | Q(phone_number_2=value))

    def filter_partner_name(self, queryset: QuerySet, name: str, value: str) -> QuerySet:
        return queryset.filter(company__partner__name=value.upper())

    def filter_partner_doc(self, queryset: QuerySet, name: str, value: str) -> QuerySet:
        CPF_LENGTH = 11

        tax_id = re.sub(r'\D', '', value)
        if len(tax_id) == CPF_LENGTH:
            tax_id = f'***{tax_id[3:9]}**'

        return queryset.filter(company__partner__cpf_cnpj=tax_id)

    class Meta:
        model = Establishment
        fields = []
