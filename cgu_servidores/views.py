from rest_framework.viewsets import GenericViewSet
from rest_framework.mixins import ListModelMixin
from .models import Employee, Retired, Pensioner
from .serializers import EmployeeSerializer, RetiredSerializer, PensionerSerializer


class EmployeeViewSet(GenericViewSet, ListModelMixin):
    queryset = Employee.objects.all()
    serializer_class = EmployeeSerializer


class RetiredViewSet(GenericViewSet, ListModelMixin):
    queryset = Retired.objects.all()
    serializer_class = RetiredSerializer


class PensionerViewSet(GenericViewSet, ListModelMixin):
    queryset = Pensioner.objects.all()
    serializer_class = PensionerSerializer
