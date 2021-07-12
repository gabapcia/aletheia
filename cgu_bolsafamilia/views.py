from rest_framework.viewsets import GenericViewSet
from rest_framework.mixins import ListModelMixin
from .models import Payment, Withdraw
from .serializers import PaymentSerializer, WithdrawSerializer


class PaymentViewSet(GenericViewSet, ListModelMixin):
    queryset = Payment.objects.all()
    serializer_class = PaymentSerializer


class WithdrawViewSet(GenericViewSet, ListModelMixin):
    queryset = Withdraw.objects.all()
    serializer_class = WithdrawSerializer
