from typing import List
import uuid
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import Column
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType


spark = SparkSession.builder.appName('TSE CANDIDATOS').getOrCreate()


# Configs
CANDIDATE_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.candidate'))
ASSETS_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.assets'))
CANCELLATION_REASON_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.cancellation_reason'))


# Schemas mapper
candidate_schema = {
    'DT_GERACAO': 'generation_date',
    'HH_GERACAO': 'generation_hour',
    'ANO_ELEICAO': 'election_year',
    'CD_TIPO_ELEICAO': 'election_type_code',
    'NM_TIPO_ELEICAO': 'election_type',
    'CD_ELEICAO': 'election_code',
    'DS_ELEICAO': 'election_description',
    'SG_UF': 'federative_unit_initials',
    'SG_UE': 'electoral_unit_initials',
    'NM_UE': 'electoral_unit',

    'NR_TURNO': 'electoral_shift_number',
    'DT_ELEICAO': 'election_date',
    'TP_ABRANGENCIA': 'election_scope',
    'CD_CARGO': 'electoral_office_code',
    'DS_CARGO': 'electoral_office',
    'SQ_CANDIDATO': 'candidate_sequential_number',
    'NR_CANDIDATO': 'number',
    'NM_CANDIDATO': 'name',
    'NM_URNA_CANDIDATO': 'voting_machine_name',
    'NM_SOCIAL_CANDIDATO': 'social_name',
    'NR_CPF_CANDIDATO': 'cpf',
    'NM_EMAIL': 'email',
    'CD_SITUACAO_CANDIDATURA': 'candidacy_situation_code',
    'DS_SITUACAO_CANDIDATURA': 'candidacy_situation',
    'CD_DETALHE_SITUACAO_CAND': 'situation_detail_code',
    'DS_DETALHE_SITUACAO_CAND': 'situation_detail',
    'TP_AGREMIACAO': 'candidacy_organization_type',
    'NR_PARTIDO': 'party_number',
    'SG_PARTIDO': 'party_initial',
    'NM_PARTIDO': 'party',
    'SQ_COLIGACAO': 'coalition_sequential_number',
    'NM_COLIGACAO': 'coalition',
    'DS_COMPOSICAO_COLIGACAO': 'coalition_composition',
    'CD_NACIONALIDADE': 'nationality_code',
    'DS_NACIONALIDADE': 'nationality',
    'SG_UF_NASCIMENTO': 'birth_federative_unit_initials',
    'CD_MUNICIPIO_NASCIMENTO': 'birth_federative_unit_code',
    'NM_MUNICIPIO_NASCIMENTO': 'birth_federative_unit',
    'DT_NASCIMENTO': 'birthday',
    'NR_IDADE_DATA_POSSE': 'age_at_possession_date',
    'NR_TITULO_ELEITORAL_CANDIDATO': 'voter_registration_number',
    'CD_GENERO': 'gender_code',
    'DS_GENERO': 'gender',
    'CD_GRAU_INSTRUCAO': 'education_degree_code',
    'DS_GRAU_INSTRUCAO': 'education_degree',
    'CD_ESTADO_CIVIL': 'marital_status_code',
    'DS_ESTADO_CIVIL': 'marital_status',
    'CD_COR_RACA': 'ethnicity_code',
    'DS_COR_RACA': 'ethnicity',
    'CD_OCUPACAO': 'occupation_code',
    'DS_OCUPACAO': 'occupation',
    'VR_DESPESA_MAX_CAMPANHA': 'max_amount_expenses',
    'CD_SIT_TOT_TURNO': 'totalization_situation_code',
    'DS_SIT_TOT_TURNO': 'totalization_situation',
    'ST_REELEICAO': 'is_reelection',
    'ST_DECLARAR_BENS': 'has_assets',
    'NR_PROTOCOLO_CANDIDATURA': 'candidacy_protocol_number',
    'NR_PROCESSO': 'process_number',
    'CD_SITUACAO_CANDIDATO_PLEITO': 'status_on_election_date_code',
    'DS_SITUACAO_CANDIDATO_PLEITO': 'status_on_election_date',
    'CD_SITUACAO_CANDIDATO_URNA': 'status_on_voting_machine_code',
    'DS_SITUACAO_CANDIDATO_URNA': 'status_on_voting_machine',
    'ST_CANDIDATO_INSERIDO_URNA': 'available_in_voting_machine',
    'SG_FEDERACAO': 'federation_initials',
    'NR_FEDERACAO': 'federation_number',
    'NM_FEDERACAO': 'federation',
    'CD_SITUACAO_CANDIDATO_TOT': 'status_in_totalization_database_code',
    'DS_SITUACAO_CANDIDATO_TOT': 'status_in_totalization_database',
    'NM_TIPO_DESTINACAO_VOTOS': 'votes_destination_type',
    'DS_COMPOSICAO_FEDERACAO': 'parties_in_federation',
    'ST_PREST_CONTAS': 'is_accountability_situation_ok',
}

assets_schema = {
    'DT_GERACAO': 'generation_date',
    'HH_GERACAO': 'generation_hour',
    'ANO_ELEICAO': 'election_year',
    'CD_TIPO_ELEICAO': 'election_type_code',
    'NM_TIPO_ELEICAO': 'election_type',
    'CD_ELEICAO': 'election_code',
    'DS_ELEICAO': 'election_description',
    'DT_ELEICAO': 'election_date',
    'SG_UF': 'federative_unit_initials',
    'SG_UE': 'electoral_unit_initials',
    'NM_UE': 'electoral_unit',

    'SQ_CANDIDATO': 'candidate_sequential_number',
    'NR_ORDEM_CANDIDATO': 'order_number',
    'CD_TIPO_BEM_CANDIDATO': 'asset_type_code',
    'DS_TIPO_BEM_CANDIDATO': 'asset_type',
    'DS_BEM_CANDIDATO': 'asset',
    'VR_BEM_CANDIDATO': 'value',
    'DT_ULTIMA_ATUALIZACAO': 'last_update_date',
    'HH_ULTIMA_ATUALIZACAO': 'last_update_hour',
}

cancellation_reason_schema = {
    'DT_GERACAO': 'generation_date',
    'HH_GERACAO': 'generation_hour',
    'ANO_ELEICAO': 'election_year',
    'CD_TIPO_ELEICAO': 'election_type_code',
    'NM_TIPO_ELEICAO': 'election_type',
    'CD_ELEICAO': 'election_code',
    'DS_ELEICAO': 'election_description',
    'SG_UF': 'federative_unit_initials',
    'SG_UE': 'electoral_unit_initials',
    'NM_UE': 'electoral_unit',

    'SQ_CANDIDATO': 'candidate_sequential_number',
    'DS_MOTIVO_CASSACAO': 'cancellation_reason',
}


# Loading the datasets
candidate_df = (
    spark.read
    .options(
        delimiter=';',
        quote='"',
        encoding='ISO-8859-1',
        inferSchema=True,
        header=True,
    )
    .csv(f's3a://{CANDIDATE_BUCKET_PATH}')
)
for key, translate in candidate_schema.items():
    if key in candidate_df.columns:
        candidate_df = candidate_df.withColumnRenamed(key, translate)
    else:
        candidate_df = candidate_df.withColumn(translate, f.lit('#NULO#'))

assets_df = spark.createDataFrame(data=[], schema=StructType([
    StructField(col, StringType(), nullable=True)
    for col in assets_schema.values()
]))
if ASSETS_BUCKET_PATH:
    assets_df = (
        spark.read
        .options(
            delimiter=';',
            quote='"',
            encoding='ISO-8859-1',
            inferSchema=True,
            header=True,
        )
        .csv(f's3a://{ASSETS_BUCKET_PATH}')
    )

    if not set(assets_df.columns).issubset(set(assets_schema.keys())):
        raise Exception('Missing assets keys')

    for key, translate in assets_schema.items():
        if key in assets_df.columns:
            assets_df = assets_df.withColumnRenamed(key, translate)
        else:
            assets_df = assets_df.withColumn(translate, f.lit('#NULO#'))

cancellation_reason_df = spark.createDataFrame(data=[], schema=StructType([
    StructField(col, StringType(), nullable=True)
    for col in cancellation_reason_schema.values()
]))
if CANCELLATION_REASON_BUCKET_PATH:
    cancellation_reason_df = (
        spark.read
        .options(
            delimiter=';',
            quote='"',
            encoding='ISO-8859-1',
            inferSchema=True,
            header=True,
        )
        .csv(f's3a://{CANCELLATION_REASON_BUCKET_PATH}')
    )

    if not set(cancellation_reason_df.columns).issubset(set(cancellation_reason_schema.keys())):
        raise Exception('Missing cancellation reason keys')

    for key, translate in cancellation_reason_schema.items():
        if key in cancellation_reason_df.columns:
            cancellation_reason_df = cancellation_reason_df.withColumnRenamed(key, translate)
        else:
            cancellation_reason_df = cancellation_reason_df.withColumn(translate, f.lit('#NULO#'))


# Clean fields
null_values = ['#NULO', '#NULO#', '#NE', '#NE#', -1, -3]

for col in candidate_df.columns:
    candidate_df = candidate_df.withColumn(col, f.when(~f.col(col).isin(null_values), f.col(col)))

for col in assets_df.columns:
    assets_df = assets_df.withColumn(col, f.when(~f.col(col).isin(null_values), f.col(col)))

for col in cancellation_reason_df.columns:
    cancellation_reason_df = cancellation_reason_df.withColumn(col, f.when(~f.col(col).isin(null_values), f.col(col)))


# Formating dates
candidate_df = (
    candidate_df
    .withColumn(
        'generation_date',
        f.to_timestamp(f.concat_ws(' ', 'generation_date', 'generation_hour'), 'dd/MM/yyyy HH:mm:ss')
    )
    .withColumn(
        'election_date',
        f.when(f.trim(f.col('election_date')).rlike('^\d{2}/\d{2}/\d{4}$'), f.to_date('election_date', 'dd/MM/yyyy'))
    )
    .withColumn(
        'birthday',
        f.when(f.trim(f.col('birthday')).rlike('^\d{2}/\d{2}/\d{4}$'), f.to_date('birthday', 'dd/MM/yyyy'))
    )
    .drop('generation_hour')
)

assets_df = (
    assets_df
    .withColumn(
        'generation_date',
        f.to_timestamp(f.concat_ws(' ', 'generation_date', 'generation_hour'), 'dd/MM/yyyy HH:mm:ss')
    )
    .withColumn(
        'last_update_date',
        f.to_timestamp(f.concat_ws(' ', 'last_update_date', 'last_update_hour'), 'dd/MM/yyyy HH:mm:ss')
    )
    .withColumn('election_date', f.to_date('election_date', 'dd/MM/yyyy'))
    .drop('generation_hour', 'last_update_hour')
)

cancellation_reason_df = (
    cancellation_reason_df
    .withColumn(
        'generation_date',
        f.to_timestamp(f.concat_ws(' ', 'generation_date', 'generation_hour'), 'dd/MM/yyyy HH:mm:ss')
    )
    .drop('generation_hour')
)

# Parsing boolean fields
candidate_df = (
    candidate_df
    .withColumn(
        'is_reelection',
        f.when(f.col('is_reelection') == 'S', True)
            .otherwise(f.when(f.col('is_reelection') == 'N', False))
    )
    .withColumn(
        'has_assets',
        f.when(f.col('has_assets') == 'S', True)
            .otherwise(f.when(f.col('has_assets') == 'N', False))
    )
    .withColumn(
        'available_in_voting_machine',
        f.when(f.col('available_in_voting_machine') == 'S', True)
            .otherwise(f.when(f.col('available_in_voting_machine') == 'N', False))
    )
    .withColumn(
        'is_accountability_situation_ok',
        f.when(f.col('is_accountability_situation_ok') == 'S', True)
            .otherwise(f.when(f.col('is_accountability_situation_ok') == 'N', False))
    )
)

# Formating name
candidate_df = (
    candidate_df
    .withColumn('name', f.upper('name'))
    .withColumn('social_name', f.upper('social_name'))
)


# Formatting tax ids
candidate_df = (
    candidate_df
    .withColumn('cpf', f.regexp_replace(f.col('cpf'), r'[^\d\*]', ''))
)


# Creating the unique ID
id_fields = [
    'election_year',
    'election_type_code',
    'election_code',
    'federative_unit_initials',
    'electoral_unit_initials',
    'candidate_sequential_number',
]

@f.udf(StringType())
def generate_id(*args: List[str]) -> str:
    name = ':'.join([str(a) for a in args])

    id = uuid.uuid5(uuid.NAMESPACE_OID, name)
    return str(id)


candidate_df = candidate_df.withColumn('candidate_id', generate_id(*id_fields))
assets_df = assets_df.withColumn('candidate_id', generate_id(*id_fields))
cancellation_reason_df = cancellation_reason_df.withColumn('candidate_id', generate_id(*id_fields))


# Drop duplicates
candidate_df = candidate_df.dropDuplicates(['candidate_id'])
cancellation_reason_df = cancellation_reason_df.dropDuplicates(['candidate_id'])
assets_df = assets_df.dropDuplicates(['candidate_id', 'order_number'])


# Parsing monetary values
def brl_string_to_monetary(col: Column) -> Column:
    col = f.regexp_replace(col, r'\.', '')
    col = f.regexp_replace(col, r',', '.')
    col = col.cast(FloatType()) * 100
    col = f.round(col, 0).cast(LongType())
    return col


assets_df = (
    assets_df
    .withColumn('value', brl_string_to_monetary(f.col('value')))
)


# Ensure data types
candidate_df = (
    candidate_df
    .withColumn('candidate_sequential_number', f.col('candidate_sequential_number').cast(LongType()))
    .withColumn('number', f.col('number').cast(LongType()))
    .withColumn('party_number', f.col('party_number').cast(LongType()))
    .withColumn('coalition_sequential_number', f.col('coalition_sequential_number').cast(LongType()))
    .withColumn('age_at_possession_date', f.col('age_at_possession_date').cast(LongType()))
    .withColumn('voter_registration_number', f.col('voter_registration_number').cast(LongType()))
    .withColumn('candidacy_protocol_number', f.col('candidacy_protocol_number').cast(LongType()))
    .withColumn('process_number', f.col('process_number').cast(LongType()))
    .withColumn('federation_number', f.col('federation_number').cast(LongType()))
)


# Formating the datasets
assets_df = (
    assets_df
    .select(
        'candidate_id',
        f.struct(
            'order_number',
            'asset_type_code',
            'asset_type',
            'asset',
            'value',
            'last_update_date',
        ).alias('asset')
    )
    .groupBy('candidate_id')
    .agg(f.collect_list('asset').alias('assets'))
)

cancellation_reason_df = (
    cancellation_reason_df
    .select('candidate_id', 'cancellation_reason')
)


# Joining datasets
candidate_df = (
    candidate_df
    .join(assets_df, 'candidate_id', 'left')
    .join(cancellation_reason_df, 'candidate_id', 'left')
)


# Saving to ElasticSearch
index_name = str(spark.conf.get('spark.es.resource'))

candidate_df.write.format('org.elasticsearch.spark.sql')\
    .mode('overwrite')\
    .option('es.resource', index_name)\
    .option('es.write.operation', 'index')\
    .option('es.mapping.id', 'candidate_id')\
    .option('es.mapping.exclude', 'candidate_id')\
    .save()
