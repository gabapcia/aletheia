from rest_framework.viewsets import GenericViewSet
from rest_framework.mixins import ListModelMixin
from drf_spectacular.utils import extend_schema, OpenApiParameter
from .models import Payment, Withdraw
from .serializers import PaymentSerializer, WithdrawSerializer
from .filters import PaymentFilter, WithdrawFilter


class PaymentViewSet(GenericViewSet, ListModelMixin):
    queryset = Payment.objects.all()
    serializer_class = PaymentSerializer
    filterset_class = PaymentFilter

    @extend_schema(
        operation_id='Listar pagamentos do Bolsa Família',
        parameters=[
            OpenApiParameter(name='page', type=int, description='Número da página a ser retornada', default=0),
            OpenApiParameter(name='limit', type=int, description='Número máximo de resultados por página', default=10),
        ],
        description='Listar dados de pagamentos do Bolsa Família fornecidos pela Controladoria-Geral da União (CGU)',
    )
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)


class WithdrawViewSet(GenericViewSet, ListModelMixin):
    queryset = Withdraw.objects.all()
    serializer_class = WithdrawSerializer
    filterset_class = WithdrawFilter

    @extend_schema(
        operation_id='Listar saques do Bolsa Família',
        parameters=[
            OpenApiParameter(name='page', type=int, description='Número da página a ser retornada', default=0),
            OpenApiParameter(name='limit', type=int, description='Número máximo de resultados por página', default=10),
        ],
        description='Listar dados de saques do Bolsa Família fornecidos pela Controladoria-Geral da União (CGU)',
    )
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)
