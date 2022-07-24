from pyspark.sql import SparkSession
from pyspark.sql import Column
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType, IntegerType


spark = SparkSession.builder.appName('CGU SERVIDORES - Employee').getOrCreate()


# Configs
REGISTER_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.register'))
SALARY_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.salary'))
REMOVED_FROM_POSITION_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.removal', ''))
OBSERVATION_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.observation'))
HONORARY_ADVOCATIVE_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.honorary.advocative', ''))
HONORARY_JETONS_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.honorary.jetons', ''))


# region Honoraries
# Schemas
honorary_advocative_schema = StructType([
    StructField('ano', StringType(), nullable=False),
    StructField('mes', StringType(), nullable=False),
    StructField('id_portal', StringType(), nullable=False),
    StructField('cpf', StringType(), nullable=False),
    StructField('nome', StringType(), nullable=False),
    StructField('observacao', StringType(), nullable=False),
    StructField('valor', StringType(), nullable=False),
])

honorary_jetons_schema = StructType([
    StructField('ano', StringType(), nullable=False),
    StructField('mes', StringType(), nullable=False),
    StructField('id_portal', StringType(), nullable=False),
    StructField('cpf', StringType(), nullable=False),
    StructField('nome', StringType(), nullable=False),
    StructField('empresa', StringType(), nullable=False),
    StructField('valor', StringType(), nullable=False),
])


# Loading the datasets
honorary_advocative_df = spark.createDataFrame(data=list(), schema=honorary_advocative_schema)
if HONORARY_ADVOCATIVE_BUCKET_PATH:
    honorary_advocative_df = spark.read\
        .schema(honorary_advocative_schema)\
        .options(
            header='true',
            delimiter=';',
            quote='"',
            encoding='ISO-8859-1',
        )\
        .csv(f's3a://{HONORARY_ADVOCATIVE_BUCKET_PATH}')\
        .select(
            'id_portal',
            'observacao',
            'valor',
            f.concat_ws('-', 'ano', f.lpad('mes', 2, '0')).alias('ano_mes')
        )

honorary_jetons_df = spark.createDataFrame(data=list(), schema=honorary_jetons_schema)
if HONORARY_JETONS_BUCKET_PATH:
    honorary_jetons_df = spark.read\
        .schema(honorary_jetons_schema)\
        .options(
            header='true',
            delimiter=';',
            quote='"',
            encoding='ISO-8859-1',
        )\
        .csv(f's3a://{HONORARY_JETONS_BUCKET_PATH}')\
        .select(
            'id_portal',
            'empresa',
            'valor',
            f.concat_ws('-', 'ano', f.lpad('mes', 2, '0')).alias('ano_mes')
        )


# Parsing monetary values
def brl_string_to_monetary(col: Column) -> Column:
    col = f.regexp_replace(col, r'\.', '')
    col = f.regexp_replace(col, r',', '.')
    col = col.cast(FloatType()) * 100
    col = f.round(col, 0).cast(IntegerType())
    return col


honorary_advocative_df = honorary_advocative_df\
    .withColumn('valor', brl_string_to_monetary(f.col('valor')))

honorary_jetons_df = honorary_jetons_df\
    .withColumn('valor', brl_string_to_monetary(f.col('valor')))


# Deduplicate entries
honorary_advocative_df = (
    honorary_advocative_df
    .select(
        'id_portal',
        f.struct(*[col for col in honorary_advocative_df.columns if col != 'id_portal']).alias('advocaticio'),
    )
    .groupBy('id_portal')
    .agg(f.collect_list('advocaticio').alias('advocaticios'))
)

honorary_jetons_df = (
    honorary_jetons_df
    .select(
        'id_portal',
        f.struct(*[col for col in honorary_jetons_df.columns if col != 'id_portal']).alias('jetons'),
    )
    .groupBy('id_portal')
    .agg(f.collect_list('jetons').alias('jetons'))
)


# Joining datasets
honoraries_df = (
    honorary_advocative_df
    .join(honorary_jetons_df, 'id_portal', 'full')
    .na.drop(how='any', subset=['id_portal'])
    .select('id_portal', f.struct('jetons', 'advocaticios').alias('honorarios'))
)

# endregion


# Schemas
# Source: https://www.portaltransparencia.gov.br/pagina-interna/603422-dicionario-de-dados-servidores-cadastro
register_schema = StructType([
    StructField('id_portal', StringType(), nullable=False),
    StructField('nome', StringType(), nullable=False),
    StructField('cpf', StringType(), nullable=False),
    StructField('matricula', StringType(), nullable=False),
    StructField('descricao_cargo', StringType(), nullable=False),
    StructField('classe_cargo', StringType(), nullable=False),
    StructField('referencia_cargo', StringType(), nullable=False),
    StructField('padrao_cargo', StringType(), nullable=False),
    StructField('nivel_cargo', StringType(), nullable=False),
    StructField('sigla_funcao', StringType(), nullable=False),
    StructField('nivel_funcao', StringType(), nullable=False),
    StructField('funcao', StringType(), nullable=False),
    StructField('codigo_atividade', StringType(), nullable=False),
    StructField('atividade', StringType(), nullable=False),
    StructField('opcao_parcial', StringType(), nullable=False),
    StructField('codigo_unidade_orgao_lotacao', StringType(), nullable=False),
    StructField('unidade_orgao_lotacao', StringType(), nullable=False),
    StructField('codigo_orgao_lotacao', StringType(), nullable=False),
    StructField('orgao_lotacao', StringType(), nullable=False),
    StructField('codigo_orgao_superior_lotacao', StringType(), nullable=False),
    StructField('orgao_superior_lotacao', StringType(), nullable=False),
    StructField('codigo_unidade_orgao_exercicio', StringType(), nullable=False),
    StructField('unidade_orgao_exercicio', StringType(), nullable=False),
    StructField('codigo_orgao_exercicio', StringType(), nullable=False),
    StructField('orgao_exercicio', StringType(), nullable=False),
    StructField('codigo_orgao_superior_exercicio', StringType(), nullable=False),
    StructField('orgao_superior_exercicio', StringType(), nullable=False),
    StructField('codigo_tipo_vinculo', StringType(), nullable=False),
    StructField('tipo_vinculo', StringType(), nullable=False),
    StructField('situacao_vinculo', StringType(), nullable=False),
    StructField('data_inicio_afastamento', DateType(), nullable=False),
    StructField('data_termino_afastamento', DateType(), nullable=False),
    StructField('regime_juridico', StringType(), nullable=False),
    StructField('jornada_trabalho', StringType(), nullable=False),
    StructField('data_ingresso_cargo_funcao', DateType(), nullable=False),
    StructField('data_nomeacao_cargo_funcao', DateType(), nullable=False),
    StructField('data_ingresso_orgao', DateType(), nullable=False),
    StructField('documento_ingresso_servico_publico', StringType(), nullable=False),
    StructField('data_diploma_ingresso', DateType(), nullable=False),
    StructField('diploma_ingresso_cargo_funcao', StringType(), nullable=False),
    StructField('diploma_ingresso_orgao', StringType(), nullable=False),
    StructField('diploma_ingresso_servico_publico', StringType(), nullable=False),
    StructField('uf_exercicio', StringType(), nullable=False),
])

# Source: https://www.portaltransparencia.gov.br/pagina-interna/603423-dicionario-de-dados-servidores-remuneracao
salary_schema = StructType([
    StructField('ano', IntegerType(), nullable=False),
    StructField('mes', IntegerType(), nullable=False),
    StructField('id_portal', StringType(), nullable=False),
    StructField('cpf', StringType(), nullable=False),
    StructField('nome', StringType(), nullable=False),
    StructField('remuneracao_basica_bruca', StringType(), nullable=False),
    StructField('remuneracao_basica_bruca_dolar', StringType(), nullable=False),
    StructField('abate_teto', StringType(), nullable=False),
    StructField('abate_teto_dolar', StringType(), nullable=False),
    StructField('gratificacao_natalina', StringType(), nullable=False),
    StructField('gratificacao_natalina_dolar', StringType(), nullable=False),
    StructField('abate_teto_gratificacao_natalina', StringType(), nullable=False),
    StructField('abate_teto_gratificacao_natalina_dolar', StringType(), nullable=False),
    StructField('ferias', StringType(), nullable=False),
    StructField('ferias_dolar', StringType(), nullable=False),
    StructField('outras_reuneracoes_eventuais', StringType(), nullable=False),
    StructField('outras_reuneracoes_eventuais_dolar', StringType(), nullable=False),
    StructField('irrf', StringType(), nullable=False),
    StructField('irrf_dolar', StringType(), nullable=False),
    StructField('pss_rpgs', StringType(), nullable=False),
    StructField('pss_rpgs_dolar', StringType(), nullable=False),
    StructField('demais_deducoes', StringType(), nullable=False),
    StructField('demais_deducoes_dolar', StringType(), nullable=False),
    StructField('pensao_militar', StringType(), nullable=False),
    StructField('pensao_militar_dolar', StringType(), nullable=False),
    StructField('fundo_saude', StringType(), nullable=False),
    StructField('fundo_saude_dolar', StringType(), nullable=False),
    StructField('taxa_ocupacao_imovel_funcional', StringType(), nullable=False),
    StructField('taxa_ocupacao_imovel_funcional_dolar', StringType(), nullable=False),
    StructField('remuneracao_apos_deducoes_obrigatorias', StringType(), nullable=False),
    StructField('remuneracao_apos_deducoes_obrigatorias_dolar', StringType(), nullable=False),
    StructField('verbas_indenizatorias', StringType(), nullable=False),
    StructField('verbas_indenizatorias_dolar', StringType(), nullable=False),
    StructField('verbas_indenizatorias_militar', StringType(), nullable=False),
    StructField('verbas_indenizatorias_militar_dolar', StringType(), nullable=False),
    StructField('verbas_indenizatorias_desligamento_voluntario', StringType(), nullable=False),
    StructField('verbas_indenizatorias_desligamento_voluntario_dolar', StringType(), nullable=False),
    StructField('verbas_indenizatorias_total', StringType(), nullable=False),
    StructField('verbas_indenizatorias_total_dolar', StringType(), nullable=False),
])

removal_schema = StructType([
    StructField('ano', IntegerType(), nullable=False),
    StructField('mes', IntegerType(), nullable=False),
    StructField('id_portal', StringType(), nullable=False),
    StructField('cpf', StringType(), nullable=False),
    StructField('nome', StringType(), nullable=False),
    StructField('data_inicio_afastamento', DateType(), nullable=False),
    StructField('data_termino_afastamento', DateType(), nullable=False),
])

observation_schema = StructType([
    StructField('ano', IntegerType(), nullable=False),
    StructField('mes', IntegerType(), nullable=False),
    StructField('id_portal', StringType(), nullable=False),
    StructField('nome', StringType(), nullable=False),
    StructField('cpf', StringType(), nullable=False),
    StructField('observacao', StringType(), nullable=False),
])


# Loading the dataset
register_df = spark.read\
    .schema(register_schema)\
    .options(
        header='true',
        delimiter=';',
        quote='"',
        encoding='ISO-8859-1',
        dateFormat='dd/MM/yyyy',
    )\
    .csv(f's3a://{REGISTER_BUCKET_PATH}')

salary_df = spark.read\
    .schema(salary_schema)\
    .options(
        header='true',
        delimiter=';',
        quote='"',
        encoding='ISO-8859-1',
    )\
    .csv(f's3a://{SALARY_BUCKET_PATH}')\
    .na.drop(how='any', subset=['ano', 'mes'])\
    .withColumn('ano_mes', f.lpad('mes', 2, '0'))\
    .drop('ano', 'mes')

observation_df = spark.read\
    .schema(observation_schema)\
    .options(
        header='true',
        delimiter=';',
        quote='"',
        encoding='ISO-8859-1',
    )\
    .csv(f's3a://{OBSERVATION_BUCKET_PATH}')\
    .select(
        'id_portal',
        'observacao',
        f.concat_ws('-', 'ano', f.lpad('mes', 2, '0')).alias('ano_mes'),
    )

removal_df = spark.createDataFrame(data=list(), schema=removal_schema)
if REMOVED_FROM_POSITION_BUCKET_PATH:
    removal_df = spark.read\
        .schema(removal_schema)\
        .options(
            header='true',
            delimiter=';',
            quote='"',
            encoding='ISO-8859-1',
            dateFormat='dd/MM/yyyy',
        )\
        .csv(f's3a://{REMOVED_FROM_POSITION_BUCKET_PATH}')\
        .select(
            'id_portal',
            'data_inicio_afastamento',
            'data_termino_afastamento',
            f.concat_ws('-', 'ano', f.lpad('mes', 2, '0')).alias('ano_mes')
        )


# Formatting tax ids
register_df = register_df\
    .withColumn('cpf', f.regexp_replace(f.col('cpf'), r'[^\d\*]', ''))


# Uppercasing names
register_df = register_df\
    .withColumn('nome', f.upper(f.col('nome')))


# Trim all fields
for col in register_df.columns:
    register_df = register_df.withColumn(col, f.trim(col))


# Parsing monetary values
not_monetary_columns = ['ano_mes', 'id_portal', 'cpf', 'nome']

for col in salary_df.columns:
    if col in not_monetary_columns:
        continue

    salary_df = salary_df.withColumn(col, brl_string_to_monetary(f.col(col)))


# Deduplicate entries
not_position_related_fields = ['id_portal', 'nome', 'cpf', 'matricula']

register_df = (
    register_df
    .select(
        f.struct(*[col for col in register_df.columns if col not in not_position_related_fields]).alias('cargo'),
        *not_position_related_fields,
    )
    .groupBy(*not_position_related_fields)
    .agg(f.collect_list('cargo').alias('cargos'))
)

salary_df = (
    salary_df
    .join(observation_df, ['id_portal', 'ano_mes'], 'left')
    .select(
        'id_portal',
        f.struct(*[
            col
            for col in salary_df.columns
            if col not in (not_monetary_columns + ['ano_mes'])
        ]).alias('remuneracao'),
    )
    .groupBy('id_portal')
    .agg(f.collect_list('remuneracao').alias('remuneracoes'))
)

removal_df = (
    removal_df
    .select(
        'id_portal',
        f.struct(*[col for col in removal_df.columns if col != 'id_portal']).alias('afastamento'),
    )
    .groupBy('id_portal')
    .agg(f.collect_list('afastamento').alias('afastamentos'))
)


# Joining datasets
people_df = (
    register_df
    .join(salary_df, 'id_portal', 'left')
    .join(removal_df, 'id_portal', 'left')
    .join(honoraries_df, 'id_portal', 'left')
)


# Saving to ElasticSearch
index_name = str(spark.conf.get('spark.es.resource'))

people_df.write.format('org.elasticsearch.spark.sql')\
    .mode('append')\
    .option('es.resource', index_name)\
    .option('es.write.operation', 'index')\
    .option('es.mapping.id', 'id_portal')\
    .save()
