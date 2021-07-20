from rest_framework.viewsets import GenericViewSet
from rest_framework.mixins import ListModelMixin
from drf_spectacular.utils import extend_schema, OpenApiParameter
from .models import Person
from .serializers import PersonSerializer
from .filters import PersonFilter


class PersonViewSet(GenericViewSet, ListModelMixin):
    queryset = Person.objects.all()
    serializer_class = PersonSerializer
    filterset_class = PersonFilter

    @extend_schema(
        operation_id='Listar beneficiários do Auxílio Emergencial',
        parameters=[
            OpenApiParameter(name='page', type=int, description='Número da página a ser retornada', default=0),
            OpenApiParameter(name='limit', type=int, description='Número máximo de resultados por página', default=10),
        ],
        description='Listar beneficiários do Auxílio Emergencial registrados na Controladoria-Geral da União (CGU)',
    )
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)
