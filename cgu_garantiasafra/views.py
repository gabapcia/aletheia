from rest_framework.viewsets import GenericViewSet
from rest_framework.mixins import ListModelMixin
from drf_spectacular.utils import extend_schema, OpenApiParameter
from .models import Warranty
from .serializers import WarrantySerializer
from .filters import WarrantyFilter


class WarrantyViewSet(GenericViewSet, ListModelMixin):
    queryset = Warranty.objects.all()
    serializer_class = WarrantySerializer
    filterset_class = WarrantyFilter

    @extend_schema(
        operation_id='Listar parcelas do Garantia-Safra',
        parameters=[
            OpenApiParameter(name='page', type=int, description='Número da página a ser retornada', default=0),
            OpenApiParameter(name='limit', type=int, description='Número máximo de resultados por página', default=10),
        ],
        description='Listar parcelas do Garantia-Safra pagas pela Controladoria-Geral da União (CGU)',
    )
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)
