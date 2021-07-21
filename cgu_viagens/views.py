from rest_framework.viewsets import GenericViewSet
from rest_framework.mixins import ListModelMixin
from drf_spectacular.utils import extend_schema, OpenApiParameter
from .models import Trip
from .serializers import TripSerializer
from .filters import TripFilter


class TripViewSet(GenericViewSet, ListModelMixin):
    queryset = Trip.objects.all()
    serializer_class = TripSerializer
    filterset_class = TripFilter

    @extend_schema(
        operation_id='Listar viajens',
        parameters=[
            OpenApiParameter(name='page', type=int, description='Número da página a ser retornada', default=0),
            OpenApiParameter(name='limit', type=int, description='Número máximo de resultados por página', default=10),
        ],
        description='Listar viagens de servidores pagas pela Controladoria-Geral da União (CGU)',
    )
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)
