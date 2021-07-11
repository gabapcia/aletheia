from rest_framework.serializers import ModelSerializer
from .models import Person


class PersonSerializer(ModelSerializer):
    class Meta:
        model = Person
        exclude = ['id', 'created_at', 'updated_at', 'release']
