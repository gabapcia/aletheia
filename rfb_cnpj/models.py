import uuid
from django.db import models
from django.db.models import UniqueConstraint, Index, Q
from psqlextra.models import PostgresModel


class Release(PostgresModel):
    id = models.UUIDField(primary_key=True, editable=False, default=uuid.uuid4)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    release_date = models.DateField(unique=True)
    finished = models.BooleanField(default=False)

    folder = models.CharField(max_length=512)

    class Meta:
        indexes = [
            Index(fields=['created_at']),
            Index(fields=['updated_at']),
        ]
        constraints = [
            UniqueConstraint(
                name='unique_pendind_release',
                fields=['finished'],
                condition=Q(finished=False),
            ),
        ]


class InsertionTask(PostgresModel):
    class Type(models.IntegerChoices):
        COMPANIES = 1
        SIMPLES = 2
        ESTABLISHMENTS = 3
        PARTNERS = 4

    id = models.UUIDField(primary_key=True, editable=False, default=uuid.uuid4)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    release = models.ForeignKey(Release, on_delete=models.CASCADE)

    type = models.IntegerField(choices=Type.choices)
    finished = models.BooleanField(default=False)

    filepath = models.TextField(null=True)
    start = models.BigIntegerField()
    end = models.BigIntegerField()

    class Meta:
        indexes = [
            Index(fields=['created_at']),
            Index(fields=['updated_at']),
            Index(fields=['type']),
            Index(fields=['finished']),
        ]


class Company(PostgresModel):
    class Size(models.IntegerChoices):
        UNINFORMED = 1, 'NÃO INFORMADO'
        MICRO = 2, 'MICRO EMPRESA'
        SMALL = 3, 'EMPRESA DE PEQUENO PORTE'
        OTHER = 5, 'DEMAIS'

    id = models.UUIDField(primary_key=True, editable=False, default=uuid.uuid4)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    release = models.ForeignKey(Release, on_delete=models.CASCADE)

    base_cnpj = models.CharField(unique=True, max_length=8, help_text='CNPJ Base')
    name = models.CharField(max_length=512, help_text='Razão social')
    legal_nature = models.CharField(max_length=512, help_text='Natureza jurídica')
    qualification_person_responsible = models.CharField(max_length=512, help_text='Qualificação da pessoa responsável')
    share_capital = models.PositiveBigIntegerField(help_text='Capital social da empresa')
    size = models.IntegerField(choices=Size.choices, help_text='Porte da empresa')
    responsible_federative_entity = models.CharField(max_length=512, help_text='Ente federativo responsável')

    class Meta:
        indexes = [
            Index(fields=['created_at']),
            Index(fields=['updated_at']),
            Index(fields=['name']),
        ]


class Simples(PostgresModel):
    class SimplesOption(models.IntegerChoices):
        YES = 1, 'SIM'
        NO = 2, 'NÃO'
        OTHER = 3, 'OUTROS'

    class MeiOption(models.IntegerChoices):
        YES = 1, 'SIM'
        NO = 2, 'NÃO'
        OTHER = 3, 'OUTROS'

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    company = models.OneToOneField(Company, on_delete=models.CASCADE, primary_key=True)

    base_cnpj = models.CharField(max_length=8, unique=True, help_text='CNPJ Base')
    simples_option = models.IntegerField(choices=SimplesOption.choices, help_text='Optou pelo SIMPLES')
    simples_option_date = models.DateField(null=True, help_text='Data que optou pelo SIMPLES')
    simples_exclusion_date = models.DateField(null=True, help_text='Data de exclusão do SIMPLES')
    mei_option = models.IntegerField(choices=SimplesOption.choices, help_text='Optou pelo MEI')
    mei_option_date = models.DateField(null=True, help_text='Data que optou pelo MEI')
    mei_exclusion_date = models.DateField(null=True, help_text='Data de exclusão do MEI')

    class Meta:
        indexes = [
            Index(fields=['created_at']),
            Index(fields=['updated_at']),
            Index(fields=['simples_option']),
            Index(fields=['mei_option']),
        ]


class Establishment(PostgresModel):
    class Identifier(models.IntegerChoices):
        MATRIZ = 1, 'MATRIZ'
        FILIAL = 2, 'FILIAL'

    class RegistrationStatus(models.IntegerChoices):
        NULL = 1, 'NULA'
        ACTIVE = 2, 'ATIVA'
        SUSPENDED = 3, 'SUSPENSA'
        UNABLE = 4, 'INAPTA'
        CLOSED = 8, 'BAIXADA'

    id = models.UUIDField(primary_key=True, editable=False, default=uuid.uuid4)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    release = models.ForeignKey(Release, on_delete=models.CASCADE)

    company = models.ForeignKey(Company, on_delete=models.CASCADE)

    cnpj = models.CharField(max_length=14, unique=True, help_text='CNPJ da empresa')
    identification = models.IntegerField(choices=Identifier.choices, help_text='Identificador Matriz/Filial')
    fantasy_name = models.CharField(max_length=512, help_text='Nome fantasia')
    registration_status = models.IntegerField(choices=RegistrationStatus.choices, help_text='Situação cadastral')
    registration_status_date = models.DateField(null=True, help_text='Data da situação cadastral')
    registration_status_reason = models.CharField(max_length=512, help_text='Motivo da situação cadastral')
    city_name_abroad = models.CharField(max_length=512, help_text='Nome da cidade no exterior')
    country = models.CharField(max_length=512, help_text='País')
    activity_start_date = models.DateField(help_text='Data de início das atividades')
    main_cnae = models.CharField(max_length=7, help_text='CNAE Fiscal Principal')
    street_type = models.CharField(max_length=512, help_text='Descrição do tipo de logradouro')
    place = models.CharField(max_length=512, help_text='Logradouro onde se localiza o estabelecimento')
    number = models.CharField(max_length=100, help_text='Número onde se localiza o estabelecimento')
    complement = models.CharField(max_length=512, help_text='Complemento do endereço')
    district = models.CharField(max_length=512, help_text='Bairro onde se localiza o estabelecimento')
    zip_code = models.CharField(max_length=512, help_text='CEP do bairro onde se localiza o estabelecimento')
    federative_unit = models.CharField(max_length=512, help_text='Unidade Federativa do estabelecimento')
    county = models.CharField(max_length=512, help_text='Município onde se localiza o estabelecimento')
    phone_number_1 = models.CharField(max_length=50, help_text='Número de telefone #1')
    phone_number_2 = models.CharField(max_length=50, help_text='Número de telefone #2')
    fax_number = models.CharField(max_length=50, help_text='Número de telefone do FAX')
    email = models.CharField(max_length=256, help_text='E-mail do contribuinte')
    special_situation = models.CharField(max_length=256, help_text='Situação especial da empresa')
    special_situation_date = models.DateField(null=True, help_text='Data em que a empresa entrou em situação especial')

    class Meta:
        indexes = [
            Index(fields=['created_at']),
            Index(fields=['updated_at']),
            Index(fields=['fantasy_name']),
            Index(fields=['email']),
            Index(fields=['phone_number_1']),
            Index(fields=['phone_number_2']),
            Index(fields=['fax_number']),
        ]


class Partner(PostgresModel):
    class Identifier(models.IntegerChoices):
        PJ = 1, 'PESSOA JURÍDICA'
        PF = 2, 'PESSOA FÍSICA'
        FOREIGN = 3, 'ESTRANGEIRO'

    id = models.UUIDField(primary_key=True, editable=False, default=uuid.uuid4)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    release = models.ForeignKey(Release, on_delete=models.CASCADE)

    company = models.ForeignKey(Company, on_delete=models.CASCADE)

    identifier = models.IntegerField(choices=Identifier.choices, help_text='Código identificador de sócio')
    name = models.CharField(max_length=512, help_text='Nome do sócio')
    cpf_cnpj = models.CharField(max_length=14, help_text='CPF/CNPJ do sócio')
    qualification = models.CharField(max_length=512, help_text='Qualificação do sócio')
    join_date = models.DateField(help_text='Data de entrada na sociedade')
    country = models.CharField(null=True, max_length=100, help_text='País do sócio estrangeiro')
    legal_representative = models.CharField(max_length=512, help_text='CPF do representante legal')
    legal_representative_name = models.CharField(max_length=512, help_text='Nome do representante legal')
    legal_representative_qualification = models.CharField(max_length=512, help_text='Qualificação do representante legal')
    age_group = models.CharField(max_length=100, help_text='Faixa etária do sócio')

    class Meta:
        indexes = [
            Index(fields=['created_at']),
            Index(fields=['updated_at']),
            Index(fields=['name']),
            Index(fields=['cpf_cnpj']),
        ]
        constraints = [
            UniqueConstraint(
                name='unique_partner_per_company',
                fields=['company', 'identifier', 'name', 'cpf_cnpj', 'join_date'],
            ),
        ]
