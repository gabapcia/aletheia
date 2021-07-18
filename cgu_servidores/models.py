import uuid
from django.db import models
from psqlextra.models import PostgresModel


class Release(PostgresModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    valid = models.BooleanField(default=False)

    finished = models.BooleanField(default=False)
    folder = models.TextField()

    date = models.DateField(unique=True)
    employee_uri = models.TextField()
    retired_uri = models.TextField()
    pensioner_uri = models.TextField()

    class Meta:
        indexes = [
            models.Index(fields=['created_at']),
            models.Index(fields=['updated_at']),
            models.Index(fields=['finished']),
            models.Index(fields=['valid']),
        ]


class InsertionTask(PostgresModel):
    class Type(models.IntegerChoices):
        EMPLOYEE = 1
        RETIRED = 2
        PENSIONER = 3
        FEE = 4
        REMUNERATION = 5
        OBSERVATION = 6
        TIMEOFF = 7

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    release = models.ForeignKey(Release, on_delete=models.CASCADE)

    type = models.IntegerField(choices=Type.choices)
    finished = models.BooleanField(default=False)
    file = models.TextField()

    class Meta:
        indexes = [
            models.Index(fields=['finished']),
        ]


class Employee(PostgresModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    portal_id = models.BigIntegerField(help_text='Identificador do servidor no conjunto de arquivos')
    cpf = models.CharField(max_length=11, help_text='CPF do servidor')
    name = models.TextField(help_text='Nome do servidor')
    register = models.TextField(help_text='Matrícula do servidor')
    uf = models.CharField(max_length=2, help_text='UF de exercício do servidor')
    relationship_code = models.IntegerField(help_text='Código do tipo de vínculo')
    relationship = models.TextField(help_text='Tipo de vínculo')
    relationship_status = models.TextField(help_text='Situação do vínculo do servidor com o órgão de lotação e/ou de exercício')
    position_description = models.TextField(help_text='Descrição do cargo ocupado pelo servidor')
    position_tier = models.TextField(help_text='Classe do cargo ocupado pelo servidor')
    position_reference = models.TextField(help_text='Referência do cargo ocupado pelo servidor')
    position_default = models.TextField(help_text='Padrão do cargo ocupado pelo servidor')
    position_level = models.TextField(help_text='Nível do cargo ocupado pelo servidor')
    occupation_initials = models.TextField(help_text='Sigla do cargo/função comissionada ocupada pelo servidor')
    occupation_level = models.TextField(help_text='Nível do cargo/função comissionada ocupada pelo servidor')
    occupation = models.TextField(help_text='Descrição do cargo/função comissionada ocupada pelo servidor')
    activity_code = models.TextField(help_text='Código da atividade do cargo/função comissionada exercida pelo servidor.')
    activity = models.TextField(help_text='Descrição da atividade do cargo/função comissionada exercida pelo servidor')
    partial_option = models.TextField(
        help_text=(
            'Se servidor ocupante de cargo efetivo e de função comissionada, '
            'indica se optou pela remuneração do cargo efetivo, do posto ou graduação, ou do emprego, '
            'acrescida do percentual de 60% (sessenta por cento) do respectivo cargo em comissão'
        ),
    )
    organizational_unit_located_code = models.TextField(help_text='Código da unidade organizacional onde o servidor está lotado')
    organizational_unit_located = models.TextField(help_text='Nome da unidade organizacional onde o servidor está lotado')
    agency_located_code = models.TextField(help_text='Código do órgão onde o servidor está lotado')
    agency_located = models.TextField(help_text='Nome do órgão onde o servidor está lotado')
    higher_agency_located_code = models.TextField(help_text='Código do órgão superior onde o servidor está lotado')
    higher_agency_located = models.TextField(help_text='Nome do órgão superior onde o servidor está lotado')
    organizational_unit_office_code = models.TextField(help_text='Código da unidade organizacional onde o servidor está em exercício')
    organizational_unit_office = models.TextField(help_text='Nome da unidade organizacional onde o servidor está em exercício')
    agency_office_code = models.TextField(help_text='Código do órgão onde o servidor está em exercício')
    agency_office = models.TextField(help_text='Nome do órgão onde o servidor está em exercício')
    higher_agency_office_code = models.TextField(help_text='Código do órgão superior onde o servidor está em exercício')
    higher_agency_office = models.TextField(help_text='Nome do órgão superior onde o servidor está em exercício')
    timeoff_start_date = models.DateField(null=True, help_text='Data de início do afastamento do servidor, em caso de afastamento')
    timeoff_end_date = models.DateField(null=True, help_text='Data de término do afastamento do servidor, em caso de afastamento')
    legal_regime = models.TextField(help_text='Regime Jurídico do Servidor')
    workload = models.TextField(help_text='Jornada semanal de trabalho do servidor')
    position_admission_date = models.DateField(null=True, help_text='Data de ingresso do servidor no cargo ou função')
    position_appointment_date = models.DateField(null=True, help_text='Data de nomeação do servidor no cargo ou função')
    agency_admission_date = models.DateField(null=True, help_text='Data de ingresso do servidor no órgão')
    admission_document = models.TextField(help_text='Documento de ingresso do servidor no serviço público')
    admission_degree_date = models.DateField(null=True, help_text='Data do diploma de ingresso do servidor no serviço público')
    position_admission_degree = models.TextField(help_text='Diploma de ingresso do servidor no cargo ou função')
    agency_admission_degree = models.TextField(help_text='Diploma de ingresso do servidor no órgão')
    public_service_admission_degree = models.TextField(help_text='Diploma de ingresso do servidor no serviço público')

    class Meta:
        indexes = [
            models.Index(fields=['created_at']),
            models.Index(fields=['updated_at']),
            models.Index(fields=['portal_id']),
            models.Index(fields=['cpf']),
            models.Index(fields=['name']),
            models.Index(fields=['register']),
            models.Index(fields=['uf']),
            models.Index(fields=['position_admission_date']),
        ]
        constraints = [
            models.UniqueConstraint(
                name='unique_cgu_servidores_employee',
                fields=[
                    'portal_id',
                    'uf',
                    'relationship_code',
                    'relationship_status',
                    'position_tier',
                    'position_default',
                    'position_level',
                    'occupation_initials',
                    'activity_code',
                    'partial_option',
                    'organizational_unit_located_code',
                    'agency_located_code',
                    'higher_agency_located_code',
                    'organizational_unit_office_code',
                    'agency_office_code',
                    'higher_agency_office_code',
                    'timeoff_start_date',
                    'timeoff_end_date',
                    'legal_regime',
                    'workload',
                    'position_admission_date',
                    'position_appointment_date',
                    'agency_admission_date',
                    'admission_degree_date',
                    'public_service_admission_degree',
                ],
            ),
        ]


class Retired(PostgresModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    portal_id = models.BigIntegerField(help_text='Identificador do servidor no conjunto de arquivos')
    cpf = models.CharField(max_length=11, help_text='CPF do servidor')
    name = models.TextField(help_text='Nome do servidor')
    register = models.TextField(help_text='Matrícula do servidor')
    relationship_code = models.IntegerField(help_text='Código do tipo de vínculo')
    relationship = models.TextField(help_text='Tipo de vínculo')
    relationship_status = models.TextField(help_text='Situação do vínculo do inativo indicando, para os Militares, se Reserva ou Reforma')
    retirement_type_code = models.TextField(help_text='Código do Tipo de Aposentadoria')
    retirement_type = models.TextField(help_text='Descrição do Tipo de Aposentadoria')
    retirement_date = models.DateField(null=True, help_text='Data da aposentadoria (civis) ou de início da reserva/reforma (militares)')
    position_description = models.TextField(help_text='Descrição do cargo ocupado do inativo quando em atividade')
    organizational_unit_located_code = models.TextField(help_text='Código da unidade organizacional onde o inativo era lotado quando em atividade')
    organizational_unit_located = models.TextField(help_text='Nome da unidade organizacional onde o inativo era lotado quando em atividade')
    agency_located_code = models.TextField(help_text='Código do órgão onde o inativo era lotado quando em atividade')
    agency_located = models.TextField(help_text='Nome do órgão onde o inativo era lotado quando em atividade')
    higher_agency_located_code = models.TextField(help_text='Código do órgão superior onde o inativo era lotado quando em atividade')
    higher_agency_located = models.TextField(help_text='Nome do órgão superior onde o inativo era lotado quando em atividade')
    legal_regime = models.TextField(help_text='Regime Jurídico do inativo quando em atividade')
    workload = models.TextField(help_text='Jornada semanal de trabalho do inativo quando em atividade')
    position_admission_date = models.DateField(null=True, help_text='Data de ingresso do inativo no cargo ou função da atividade')
    position_appointment_date = models.DateField(null=True, help_text='Data de nomeação do inativo no cargo ou função da atividade')
    agency_admission_date = models.DateField(null=True, help_text='Data de ingresso do inativo no órgão')
    admission_document = models.TextField(help_text='Documento de ingresso do inativo no serviço público')
    admission_degree_date = models.DateField(null=True, help_text='Data do diploma de ingresso do inativo no serviço público')
    position_admission_degree = models.TextField(help_text='Diploma de ingresso do inativo no cargo ou função da atividade')
    agency_admission_degree = models.TextField(help_text='Diploma de ingresso do inativo no órgão')
    public_service_admission_degree = models.TextField(help_text='Diploma de ingresso do inativo no serviço público')

    class Meta:
        indexes = [
            models.Index(fields=['created_at']),
            models.Index(fields=['updated_at']),
            models.Index(fields=['portal_id']),
            models.Index(fields=['cpf']),
            models.Index(fields=['name']),
            models.Index(fields=['register']),
            models.Index(fields=['retirement_date']),
        ]
        constraints = [
            models.UniqueConstraint(
                name='unique_cgu_servidores_retired',
                fields=[
                    'portal_id',
                    'relationship_code',
                    'relationship_status',
                    'retirement_type_code',
                    'retirement_date',
                    'organizational_unit_located_code',
                    'agency_located_code',
                    'higher_agency_located_code',
                    'legal_regime',
                    'workload',
                    'position_admission_date',
                    'position_appointment_date',
                    'agency_admission_date',
                    'admission_degree_date',
                    'public_service_admission_degree',
                ],
            ),
        ]


class Pensioner(PostgresModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    portal_id = models.BigIntegerField(help_text='Identificador do servidor no conjunto de arquivos')
    cpf = models.CharField(max_length=11, help_text='CPF do servidor')
    name = models.TextField(help_text='Nome do servidor')
    register = models.TextField(help_text='Matrícula do servidor')
    relationship_code = models.IntegerField(help_text='Código do tipo de vínculo')
    relationship = models.TextField(help_text='Tipo de vínculo')
    relationship_status = models.TextField(help_text='Situação do vínculo detalhando o seu tipo')
    legal_representative_cpf = models.TextField(help_text='CPF de eventual representante legal do pensionista')
    legal_representative_name = models.TextField(help_text='Nome completo de eventual representante legal pensionista')
    pension_type_code = models.TextField(help_text='Código do Tipo de Pensão')
    pension_type = models.TextField(help_text='Descrição do Tipo de Pensão')
    pension_start_date = models.DateField(null=True, help_text='Data de início da pensão')
    pi_cpf = models.TextField(help_text='CPF do instituidor da pensão do pensionista')
    pi_name = models.TextField(help_text='Nome completo do instituidor da pensão do pensionista')
    pi_position = models.TextField(help_text='Descrição do cargo ocupado pelo instituidor da pensão quando em atividade')
    pi_organizational_unit_located_code = models.TextField(
        help_text='Código da unidade organizacional onde o instituidor da pensão era lotado quando em atividade',
    )
    pi_organizational_unit_located = models.TextField(
        help_text='Nome da unidade organizacional onde o instituidor da pensão era lotado quando em atividade',
    )
    pi_agency_office_code = models.TextField(help_text='Código do órgão onde o instituidor da pensão era lotado quando em atividade')
    pi_agency_office = models.TextField(help_text='Nome do órgão onde o instituidor da pensão era lotado quando em atividade')
    pi_higher_agency_office_code = models.TextField(help_text='Código do órgão superior onde o instituidor da pensão era lotado quando em atividade')
    pi_higher_agency_office = models.TextField(help_text='Nome do órgão superior onde o instituidor da pensão era lotado quando em atividade')
    pi_legal_regime = models.TextField(help_text='Regime Jurídico do instituidor da pensão quando em atividade')
    pi_workload = models.TextField(help_text='Jornada semanal de trabalho do instituidor da pensão quando em atividade')
    pi_position_admission_date = models.DateField(null=True, help_text='Data de ingresso do instituidor da pensão no cargo ou função da atividade')
    pi_position_appointment_date = models.DateField(null=True, help_text='Data de nomeação do instituidor da pensão no cargo ou função da atividade')
    pi_agency_admission_date = models.DateField(null=True, help_text='Data de ingresso do instituidor da pensão no órgão')
    pi_admission_document = models.TextField(help_text='Documento de ingresso do instituidor da pensão no serviço público')
    pi_admission_degree_date = models.DateField(null=True, help_text='Data do diploma de ingresso do instituidor da pensão no serviço público')
    pi_position_admission_degree = models.TextField(help_text='Diploma de ingresso do instituidor da pensão no cargo ou função da atividade')
    pi_agency_admission_degree = models.TextField(help_text='Diploma de ingresso do instituidor da pensão no órgão')
    pi_public_service_admission_degree = models.TextField(help_text='Diploma de ingresso do instituidor da pensão no serviço público')

    class Meta:
        indexes = [
            models.Index(fields=['created_at']),
            models.Index(fields=['updated_at']),
            models.Index(fields=['portal_id']),
            models.Index(fields=['cpf']),
            models.Index(fields=['name']),
            models.Index(fields=['register']),
            models.Index(fields=['legal_representative_cpf']),
            models.Index(fields=['legal_representative_name']),
            models.Index(fields=['pi_cpf']),
            models.Index(fields=['pi_name']),
            models.Index(fields=['pension_start_date']),
        ]
        constraints = [
            models.UniqueConstraint(
                name='unique_cgu_servidores_pensioner',
                fields=[
                    'portal_id',
                    'relationship_code',
                    'relationship_status',
                    'legal_representative_cpf',
                    'legal_representative_name',
                    'pension_type_code',
                    'pension_start_date',
                    'pi_cpf',
                    'pi_name',
                    'pi_organizational_unit_located_code',
                    'pi_agency_office_code',
                    'pi_higher_agency_office_code',
                    'pi_legal_regime',
                    'pi_workload',
                    'pi_position_admission_date',
                    'pi_position_appointment_date',
                    'pi_agency_admission_date',
                    'pi_admission_degree_date',
                    'pi_public_service_admission_degree',
                ],
            ),
        ]


class Fee(PostgresModel):
    class Type(models.IntegerChoices):
        ATTORNEY = 1, 'Advocatício'
        JETONS = 2, 'JETONS'

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    type = models.IntegerField(choices=Type.choices, help_text='Tipo do honorário')
    reference_date = models.DateField(help_text='Data de referência')
    portal_id = models.BigIntegerField(help_text='Identificador do servidor no conjunto de arquivos')
    cpf = models.CharField(max_length=11, help_text='CPF do servidor')
    name = models.CharField(max_length=512, help_text='Nome do servidor')
    company = models.TextField(
        help_text=(
            'Nome da empresa em que o representante da União atua em Conselhos de Administração e Fiscal ou órgãos equivalentes'
            ' - (Somente para JETONS)'
        ),
    )
    observation = models.TextField(help_text='Situação excepcional ocorrida no honorário do mês - (Somente para Advocaticios)')
    value = models.BigIntegerField(help_text='Valor do honorário recebido pelo servidor')

    class Meta:
        indexes = [
            models.Index(fields=['created_at']),
            models.Index(fields=['updated_at']),
            models.Index(fields=['type']),
            models.Index(fields=['reference_date']),
            models.Index(fields=['portal_id']),
            models.Index(fields=['cpf']),
            models.Index(fields=['name']),
            models.Index(fields=['value']),
        ]
        constraints = [
            models.UniqueConstraint(
                name='unique_cgu_servidor_fee',
                fields=[
                    'type',
                    'reference_date',
                    'portal_id',
                    'cpf',
                    'name',
                    'company',
                    'observation',
                    'value',
                ],
            ),
        ]


class Remuneration(PostgresModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    reference_date = models.DateField(help_text='Data de referência')
    portal_id = models.BigIntegerField(help_text='Identificador do servidor no conjunto de arquivos')
    cpf = models.CharField(max_length=11, help_text='CPF do servidor')
    name = models.CharField(max_length=512, help_text='Nome do servidor')
    base_remuneration_real = models.BigIntegerField(help_text='Remuneração básica bruta (R$)')
    base_remuneration_dollar = models.BigIntegerField(help_text='Remuneração básica bruta (U$)')
    ceiling_drop_real = models.BigIntegerField(help_text='Abate-teto (R$)')
    ceiling_drop_dollar = models.BigIntegerField(help_text='Abate-teto (U$)')
    christmas_bonus_real = models.BigIntegerField(help_text='Gratificação natalina (R$)')
    christmas_bonus_dollar = models.BigIntegerField(help_text='Gratificação natalina (U$)')
    ceiling_drop_christmas_bonus_real = models.BigIntegerField(help_text='Abate-teto da gratificação natalina (R$)')
    ceiling_drop_christmas_bonus_dollar = models.BigIntegerField(help_text='Abate-teto da gratificação natalina (U$)')
    vacation_real = models.BigIntegerField(help_text='Férias (R$)')
    vacation_dollar = models.BigIntegerField(help_text='Férias (U$)')
    eventual_real = models.BigIntegerField(help_text='Outras remunerações eventuais (R$)')
    eventual_dollar = models.BigIntegerField(help_text='Outras remunerações eventuais (U$)')
    irrf_real = models.BigIntegerField(help_text='IRRF (R$)')
    irrf_dollar = models.BigIntegerField(help_text='IRRF (U$)')
    social_welfare_real = models.BigIntegerField(help_text='PSS/RPGS (R$)')
    social_welfare_dollar = models.BigIntegerField(help_text='PSS/RPGS (U$)')
    other_deductions_real = models.BigIntegerField(help_text='Demais deduções (R$)')
    other_deductions_dollar = models.BigIntegerField(help_text='Demais deduções (U$)')
    military_pension_real = models.BigIntegerField(help_text='Pensão militar (R$)')
    military_pension_dollar = models.BigIntegerField(help_text='Pensão militar (U$)')
    health_fund_real = models.BigIntegerField(help_text='Fundo de saúde (R$)')
    health_fund_dollar = models.BigIntegerField(help_text='Fundo de saúde (U$)')
    property_cost_real = models.BigIntegerField(help_text='Taxa de ocupação imóvel funcional (R$)')
    property_cost_dollar = models.BigIntegerField(help_text='Taxa de ocupação imóvel funcional (U$)')
    remuneration_real = models.BigIntegerField(help_text='Remuneração após deduções obrigatórias (R$)')
    remuneration_dollar = models.BigIntegerField(help_text='Remuneração após deduções obrigatórias (U$)')
    indemnity_amount_civil_real = models.BigIntegerField(help_text='Verbas indenizatórias registradas em sistemas de pessoal - civil (R$)')
    indemnity_amount_civil_dollar = models.BigIntegerField(help_text='Verbas indenizatórias registradas em sistemas de pessoal - civil (U$)')
    indemnity_amount_militar_real = models.BigIntegerField(help_text='Verbas indenizatórias registradas em sistemas de pessoal - militar (R$)')
    indemnity_amount_militar_dollar = models.BigIntegerField(help_text='Verbas indenizatórias registradas em sistemas de pessoal - militar (U$)')
    indemnity_amount_mp_real = models.BigIntegerField(help_text='Verbas indenizatórias programa desligamento voluntário - MP 792/2017 (R$)')
    indemnity_amount_mp_dollar = models.BigIntegerField(help_text='Verbas indenizatórias programa desligamento voluntário - MP 792/2017 (U$)')
    indemnity_total_amount_real = models.BigIntegerField(help_text='Total de verbas indenizatórias (R$)')
    indemnity_total_amount_dollar = models.BigIntegerField(help_text='Total de verbas indenizatórias (U$)')

    class Meta:
        indexes = [
            models.Index(fields=['created_at']),
            models.Index(fields=['updated_at']),
            models.Index(fields=['reference_date']),
            models.Index(fields=['cpf']),
            models.Index(fields=['name']),
        ]
        constraints = [
            models.UniqueConstraint(
                name='unique_cgu_servidores_remuneration',
                fields=['reference_date', 'portal_id'],
            ),
        ]


class Observation(PostgresModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    reference_date = models.DateField(help_text='Data de referência')
    portal_id = models.BigIntegerField(help_text='Identificador do servidor no conjunto de arquivos')
    cpf = models.CharField(max_length=11, help_text='CPF do servidor')
    name = models.CharField(max_length=512, help_text='Nome do servidor')
    content = models.TextField(help_text='Situação excepcional ocorrida na remuneração do servidor no mês')

    class Meta:
        indexes = [
            models.Index(fields=['created_at']),
            models.Index(fields=['updated_at']),
            models.Index(fields=['reference_date']),
            models.Index(fields=['portal_id']),
            models.Index(fields=['cpf']),
            models.Index(fields=['name']),
        ]
        constraints = [
            models.UniqueConstraint(
                name='unique_cgu_servidores_observation',
                fields=['reference_date', 'portal_id', 'content'],
            ),
        ]


class TimeOff(PostgresModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    reference_date = models.DateField(help_text='Data de referência')
    portal_id = models.BigIntegerField(help_text='Identificador do servidor no conjunto de arquivos')
    cpf = models.CharField(max_length=11, help_text='CPF do servidor')
    name = models.CharField(max_length=512, help_text='Nome do servidor')
    start_date = models.DateField(null=True, help_text='Data de início do afastamento')
    end_date = models.DateField(null=True, help_text='Data de fim do afastamento')

    class Meta:
        indexes = [
            models.Index(fields=['created_at']),
            models.Index(fields=['updated_at']),
            models.Index(fields=['reference_date']),
            models.Index(fields=['portal_id']),
            models.Index(fields=['cpf']),
            models.Index(fields=['name']),
            models.Index(fields=['start_date']),
            models.Index(fields=['end_date']),
        ]
        constraints = [
            models.UniqueConstraint(
                name='unique_cgu_servidores_time_off',
                fields=['reference_date', 'portal_id', 'start_date', 'end_date'],
            ),
        ]
