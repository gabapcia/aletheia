from rest_framework import serializers
from .models import Warranty


class WarrantySerializer(serializers.ModelSerializer):
    class Meta:
        model = Warranty
        exclude = ['id', 'created_at', 'updated_at']
        ref_name = 'GCUGARANTIASAFRA_Warranty'
