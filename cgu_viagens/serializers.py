from rest_framework import serializers
from .models import Trip, TripPart, Ticket, Payment


class TripPartSerializer(serializers.ModelSerializer):
    class Meta:
        model = TripPart
        exclude = ['id', 'created_at', 'updated_at', 'trip']
        ref_name = 'CGUVIAGENS_TripPart'


class TicketSerializer(serializers.ModelSerializer):
    class Meta:
        model = Ticket
        exclude = ['id', 'created_at', 'updated_at', 'trip']
        ref_name = 'CGUVIAGENS_Ticket'


class PaymentSerializer(serializers.ModelSerializer):
    class Meta:
        model = Payment
        exclude = ['id', 'created_at', 'updated_at', 'trip']
        ref_name = 'CGUVIAGENS_Payment'


class TripSerializer(serializers.ModelSerializer):
    payments = PaymentSerializer(source='payment_set', many=True)
    tickets = TicketSerializer(source='ticket_set', many=True)
    parts = TripPartSerializer(source='trippart_set', many=True)

    class Meta:
        model = Trip
        exclude = ['id', 'created_at', 'updated_at']
        ref_name = 'CGUVIAGENS_Trip'
