from rest_framework.viewsets import GenericViewSet
from rest_framework.mixins import ListModelMixin
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiExample
from drf_spectacular.types import OpenApiTypes
from .serializers import EstablishmentSerializer
from .models import Establishment, Partner
from .filters import EstablishmentFilter


class EstablishmentViewSet(GenericViewSet, ListModelMixin):
    serializer_class = EstablishmentSerializer
    queryset = Establishment.objects.all()
    filterset_class = EstablishmentFilter

    @extend_schema(
        operation_id='Listar empresas',
        parameters=[
            OpenApiParameter(name='page', type=int, description='Número da página a ser retornada', default=0),
            OpenApiParameter(name='limit', type=int, description='Número máximo de resultados por página', default=10),
        ],
        description='Listar empresas registradas na Receita Federal do Brasil (RFB)',
    )
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)
