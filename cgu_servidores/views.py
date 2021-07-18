from rest_framework.viewsets import GenericViewSet
from rest_framework.mixins import ListModelMixin
from drf_spectacular.utils import extend_schema, OpenApiParameter
from .models import Employee, Retired, Pensioner
from .serializers import EmployeeSerializer, RetiredSerializer, PensionerSerializer
from .filters import EmployeeFilter, RetiredFilter, PensionerFilter


class EmployeeViewSet(GenericViewSet, ListModelMixin):
    queryset = Employee.objects.all().order_by('portal_id', '-position_admission_date').distinct('portal_id')
    serializer_class = EmployeeSerializer
    filterset_class = EmployeeFilter
    manual_ordering = True

    @extend_schema(
        operation_id='Listar servidores da CGU',
        parameters=[
            OpenApiParameter(name='page', type=int, description='Número da página a ser retornada', default=0),
            OpenApiParameter(name='limit', type=int, description='Número máximo de resultados por página', default=10),
        ],
        description='Listar servidores civis e militares registradas na Controladoria-Geral da União (CGU)',
    )
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)


class RetiredViewSet(GenericViewSet, ListModelMixin):
    queryset = Retired.objects.all().order_by('portal_id', '-retirement_date').distinct('portal_id')
    serializer_class = RetiredSerializer
    filterset_class = RetiredFilter
    manual_ordering = True

    @extend_schema(
        operation_id='Listar aposentados da CGU',
        parameters=[
            OpenApiParameter(name='page', type=int, description='Número da página a ser retornada', default=0),
            OpenApiParameter(name='limit', type=int, description='Número máximo de resultados por página', default=10),
        ],
        description='Listar aposentados civis e militares registradas na Controladoria-Geral da União (CGU)',
    )
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)


class PensionerViewSet(GenericViewSet, ListModelMixin):
    queryset = Pensioner.objects.all().order_by('portal_id', '-pension_start_date').distinct('portal_id')
    serializer_class = PensionerSerializer
    filterset_class = PensionerFilter
    manual_ordering = True

    @extend_schema(
        operation_id='Listar pensionistas da CGU',
        parameters=[
            OpenApiParameter(name='page', type=int, description='Número da página a ser retornada', default=0),
            OpenApiParameter(name='limit', type=int, description='Número máximo de resultados por página', default=10),
        ],
        description='Listar pensionistas civis e militares registradas na Controladoria-Geral da União (CGU)',
    )
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)
