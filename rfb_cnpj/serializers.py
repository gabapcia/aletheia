from rest_framework import serializers
from .models import Simples, Establishment, Partner


class SimplesSerializer(serializers.ModelSerializer):
    simples_option = serializers.CharField(source='get_simples_option_display', help_text='Optou pelo SIMPLES')
    mei_option = serializers.CharField(source='get_mei_option_display', help_text='Optou pelo MEI')

    class Meta:
        model = Simples
        exclude = ['company', 'base_cnpj', 'created_at', 'updated_at']


class PartnerSerializer(serializers.ModelSerializer):
    identifier = serializers.CharField(source='get_identifier_display')

    class Meta:
        model = Partner
        exclude = ['release', 'company', 'id', 'created_at', 'updated_at']


class EstablishmentSerializer(serializers.ModelSerializer):
    base_cnpj = serializers.CharField(
        source='company.base_cnpj',
        help_text='CNPJ Base',
    )
    name = serializers.CharField(
        source='company.name',
        help_text='Razão social',
    )
    legal_nature = serializers.CharField(
        source='company.legal_nature',
        help_text='Natureza jurídica',
    )
    qualification_person_responsible = serializers.CharField(
        source='company.qualification_person_responsible',
        help_text='Qualificação da pessoa responsável',
    )
    share_capital = serializers.IntegerField(
        source='company.share_capital',
        help_text='Capital social da empresa',
    )
    responsible_federative_entity = serializers.CharField(
        source='company.responsible_federative_entity',
        help_text='Ente federativo responsável',
    )
    size = serializers.CharField(
        source='company.get_size_display',
        help_text='Porte da empresa',
    )
    identification = serializers.CharField(source='get_identification_display', help_text='Identificador Matriz/Filial')
    registration_status = serializers.CharField(source='get_registration_status_display', help_text='Situação cadastral')
    partners = PartnerSerializer(source='company.partner_set', many=True, help_text='Sócios da empresa')
    simples = SimplesSerializer(source='company.simples', required=False, help_text='Dados do SIMPLES / MEI')

    class Meta:
        model = Establishment
        exclude = ['release', 'id', 'company', 'created_at', 'updated_at']
