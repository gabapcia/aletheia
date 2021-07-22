from django.db.models.query import QuerySet
import django_filters as filters
from .models import Warranty


class WarrantyFilter(filters.FilterSet):
    name = filters.CharFilter(method='filter_name', label='Nome do beneficiário')
    nis = filters.CharFilter(label='NIS do beneficiário')
    county_siafi_code = filters.CharFilter(label='Código SIAFI do município do beneficiário')
    uf = filters.CharFilter(label='UF do beneficiário')
    value = filters.NumberFilter(label='Valor do benefício')

    def filter_name(self, queryset: QuerySet, name: str, value: str) -> QuerySet:
        return queryset.filter(name=value.upper())

    class Meta:
        model = Warranty
        fields = []
