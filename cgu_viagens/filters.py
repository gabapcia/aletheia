import re
from django.db.models.query import QuerySet
import django_filters as filters
from .models import Trip


class TripFilter(filters.FilterSet):
    traveler_name = filters.CharFilter(method='filter_traveler_name', label='Nome do viajante')
    traveler_cpf = filters.CharFilter(method='filter_traveler_cpf', label='CPF do viajante')

    def filter_name(self, queryset: QuerySet, name: str, value: str) -> QuerySet:
        return queryset.filter(traveler_name=value.upper())

    def filter_cpf(self, queryset: QuerySet, name: str, value: str) -> QuerySet:
        tax_id = re.sub(r'\D', '', value)
        tax_id = f'***{tax_id[3:9]}**'

        return queryset.filter(traveler_cpf=tax_id)

    class Meta:
        model = Trip
        fields = []
