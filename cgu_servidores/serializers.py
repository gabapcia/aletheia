from rest_framework import serializers
from .models import Employee, Retired, Pensioner, Fee, Remuneration, Observation, TimeOff


class FeeSerializer(serializers.ModelSerializer):
    type = serializers.CharField(source='get_type_display')

    class Meta:
        model = Fee
        exclude = ['id', 'created_at', 'updated_at', 'name', 'cpf', 'portal_id']


class ObservationSerializer(serializers.ModelSerializer):
    class Meta:
        model = Observation
        exclude = ['id', 'created_at', 'updated_at', 'name', 'cpf', 'portal_id']


class RemunerationSerializer(serializers.ModelSerializer):
    observations = serializers.SerializerMethodField()

    def get_observations(self, obj: Remuneration) -> ObservationSerializer(many=True):
        observations = Observation.objects.filter(reference_date=obj.reference_date, portal_id=obj.portal_id)
        return ObservationSerializer(observations, many=True).data

    class Meta:
        model = Remuneration
        exclude = ['id', 'created_at', 'updated_at', 'portal_id', 'cpf', 'name']


class TimeOffSerializer(serializers.ModelSerializer):
    class Meta:
        model = TimeOff
        exclude = ['id', 'created_at', 'updated_at', 'name', 'cpf', 'portal_id']


class RegisterSerializer(serializers.ModelSerializer):
    fees = serializers.SerializerMethodField()
    remunerations = serializers.SerializerMethodField()
    time_offs = serializers.SerializerMethodField()

    def get_fees(self, obj) -> FeeSerializer(many=True):
        fees = Fee.objects.filter(portal_id=obj.portal_id).order_by('-reference_date')
        return FeeSerializer(fees, many=True).data

    def get_remunerations(self, obj) -> RemunerationSerializer(many=True):
        remunerations = Remuneration.objects.filter(portal_id=obj.portal_id).order_by('-reference_date')[:1]
        return RemunerationSerializer(remunerations, many=True).data

    def get_time_offs(self, obj) -> TimeOffSerializer(many=True):
        time_offs = TimeOff.objects.filter(portal_id=obj.portal_id)\
            .order_by('start_date', 'end_date', '-reference_date')\
            .distinct('start_date', 'end_date')

        return TimeOffSerializer(time_offs, many=True).data

    class Meta:
        exclude = ['id', 'created_at', 'updated_at']


class DataSerializer(serializers.ModelSerializer):
    class Meta:
        exclude = ['id', 'created_at', 'updated_at', 'portal_id', 'cpf', 'name', 'register']


class EmployeeDataSerializer(DataSerializer):
    class Meta(DataSerializer.Meta):
        model = Employee


class EmployeeSerializer(RegisterSerializer):
    history = serializers.SerializerMethodField()

    def get_history(self, obj: Employee) -> EmployeeDataSerializer(many=True):
        history = Employee.objects.filter(portal_id=obj.portal_id).exclude(pk=obj.pk).order_by('-created_at')
        return EmployeeDataSerializer(history, many=True).data

    class Meta(RegisterSerializer.Meta):
        model = Employee


class RetiredDataSerializer(DataSerializer):
    class Meta(DataSerializer.Meta):
        model = Retired


class RetiredSerializer(RegisterSerializer):
    history = serializers.SerializerMethodField()

    def get_history(self, obj: Retired) -> RetiredDataSerializer(many=True):
        history = Retired.objects.filter(portal_id=obj.portal_id).exclude(pk=obj.pk).order_by('-created_at')
        return RetiredDataSerializer(history, many=True).data

    class Meta(RegisterSerializer.Meta):
        model = Retired


class PensionerDataSerializer(DataSerializer):
    class Meta(DataSerializer.Meta):
        model = Pensioner


class PensionerSerializer(RegisterSerializer):
    history = serializers.SerializerMethodField()

    def get_history(self, obj: Pensioner) -> PensionerDataSerializer(many=True):
        history = Pensioner.objects.filter(portal_id=obj.portal_id).exclude(pk=obj.pk).order_by('-created_at')
        return PensionerDataSerializer(history, many=True).data

    class Meta(RegisterSerializer.Meta):
        model = Pensioner
