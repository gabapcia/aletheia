from rest_framework import serializers
from .models import Person


class PersonSerializer(serializers.ModelSerializer):
    class Meta:
        model = Person
        exclude = ['id', 'created_at', 'updated_at']
        ref_name = 'CGUAEPerson'
