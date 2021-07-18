from rest_framework import serializers
from .models import Employee, Retired, Pensioner, Fee, Remuneration, Observation, TimeOff


class FeeSerializer(serializers.ModelSerializer):
    type = serializers.CharField(source='get_type_display')

    class Meta:
        model = Fee
        exclude = ['id', 'created_at', 'updated_at']


class RemunerationSerializer(serializers.ModelSerializer):
    class Meta:
        model = Remuneration
        exclude = ['id', 'created_at', 'updated_at']


class ObservationSerializer(serializers.ModelSerializer):
    class Meta:
        model = Observation
        exclude = ['id', 'created_at', 'updated_at']


class TimeOffSerializer(serializers.ModelSerializer):
    class Meta:
        model = TimeOff
        exclude = ['id', 'created_at', 'updated_at']


class EmployeeSerializer(serializers.ModelSerializer):
    class Meta:
        model = Employee
        exclude = ['id', 'created_at', 'updated_at']


class RetiredSerializer(serializers.ModelSerializer):
    class Meta:
        model = Retired
        exclude = ['id', 'created_at', 'updated_at']


class PensionerSerializer(serializers.ModelSerializer):
    class Meta:
        model = Pensioner
        exclude = ['id', 'created_at', 'updated_at']
