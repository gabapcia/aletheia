from rest_framework import serializers
from .models import Payment, Withdraw


class PaymentSerializer(serializers.ModelSerializer):
    class Meta:
        model = Payment
        exclude = ['id', 'created_at', 'updated_at', 'release']



class WithdrawSerializer(serializers.ModelSerializer):
    class Meta:
        model = Withdraw
        exclude = ['id', 'created_at', 'updated_at', 'release']
