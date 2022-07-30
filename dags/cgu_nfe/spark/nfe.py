from typing import List
import uuid
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import Column
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType, IntegerType


spark = SparkSession.builder.appName('CGU NOTAS FISCAIS').getOrCreate()


# Configs
NFE_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.nfe'))
EVENT_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.event'))
ITEM_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.item'))


# Schemas
nfe_schema = StructType([
    StructField('access_key', StringType(), nullable=False),
    StructField('model', StringType(), nullable=False),
    StructField('series', StringType(), nullable=False),
    StructField('number', StringType(), nullable=False),
    StructField('operation_nature', StringType(), nullable=False),
    StructField('issue_date', TimestampType(), nullable=False),
    StructField('most_recent_event', StringType(), nullable=False),
    StructField('most_recent_event_date', TimestampType(), nullable=False),
    StructField('issuer_tax_id', StringType(), nullable=False),
    StructField('issuer_corporate_name', StringType(), nullable=False),
    StructField('issuer_state_registration', StringType(), nullable=False),
    StructField('issuer_federative_unit', StringType(), nullable=False),
    StructField('issuer_county', StringType(), nullable=False),
    StructField('recipient_cnpj', StringType(), nullable=False),
    StructField('recipient_name', StringType(), nullable=False),
    StructField('recipient_federative_unit', StringType(), nullable=False),
    StructField('recipient_state_tax', StringType(), nullable=False),
    StructField('operation_destination', StringType(), nullable=False),
    StructField('final_costumer', StringType(), nullable=False),
    StructField('buyer_presence', StringType(), nullable=False),
    StructField('value', StringType(), nullable=False),
])

event_schema = StructType([
    StructField('access_key', StringType(), nullable=False),
    StructField('model', StringType(), nullable=False),
    StructField('series', StringType(), nullable=False),
    StructField('number', StringType(), nullable=False),
    StructField('operation_nature', StringType(), nullable=False),
    StructField('issue_date', TimestampType(), nullable=False),
    StructField('event', StringType(), nullable=False),
    StructField('date', TimestampType(), nullable=False),
    StructField('description', StringType(), nullable=False),
    StructField('reason', StringType(), nullable=False),
])

item_schema = StructType([
    StructField('access_key', StringType(), nullable=False),
    StructField('model', StringType(), nullable=False),
    StructField('series', StringType(), nullable=False),
    StructField('number', StringType(), nullable=False),
    StructField('operation_nature', StringType(), nullable=False),
    StructField('issue_date', TimestampType(), nullable=False),
    StructField('issuer_tax_id', StringType(), nullable=False),
    StructField('issuer_corporate_name', StringType(), nullable=False),
    StructField('issuer_state_registration', StringType(), nullable=False),
    StructField('issuer_federative_unit', StringType(), nullable=False),
    StructField('issuer_county', StringType(), nullable=False),
    StructField('recipient_cnpj', StringType(), nullable=False),
    StructField('recipient_name', StringType(), nullable=False),
    StructField('recipient_federative_unit', StringType(), nullable=False),
    StructField('recipient_state_tax', StringType(), nullable=False),
    StructField('operation_destination', StringType(), nullable=False),
    StructField('final_costumer', StringType(), nullable=False),
    StructField('buyer_presence', StringType(), nullable=False),
    StructField('product_number', StringType(), nullable=False),
    StructField('product_description', StringType(), nullable=False),
    StructField('ncm_sh_code', StringType(), nullable=False),
    StructField('ncm_sh', StringType(), nullable=False),
    StructField('cfop', StringType(), nullable=False),
    StructField('quantity', StringType(), nullable=False),
    StructField('unit', StringType(), nullable=False),
    StructField('unit_value', StringType(), nullable=False),
    StructField('total_value', StringType(), nullable=False),
])


# Loading the dataset
nfe_df = spark.read\
    .schema(nfe_schema)\
    .options(
        header='true',
        delimiter=';',
        quote='"',
        encoding='ISO-8859-1',
        timestampFormat='dd/MM/yyyy HH:mm:ss',
    )\
    .csv(f's3a://{NFE_BUCKET_PATH}')

event_df = spark.read\
    .schema(event_schema)\
    .options(
        header='true',
        delimiter=';',
        quote='"',
        encoding='ISO-8859-1',
        timestampFormat='dd/MM/yyyy HH:mm:ss',
    )\
    .csv(f's3a://{EVENT_BUCKET_PATH}')

item_df = spark.read\
    .schema(item_schema)\
    .options(
        header='true',
        delimiter=';',
        quote='"',
        encoding='ISO-8859-1',
        timestampFormat='dd/MM/yyyy HH:mm:ss',
    )\
    .csv(f's3a://{ITEM_BUCKET_PATH}')


# Formatting tax ids
nfe_df = nfe_df\
    .withColumn('issuer_tax_id', f.regexp_replace(f.col('issuer_tax_id'), r'[^\d\*]', ''))\
    .withColumn('recipient_cnpj', f.regexp_replace(f.col('recipient_cnpj'), r'[^\d\*]', ''))

item_df = item_df\
    .withColumn('issuer_tax_id', f.regexp_replace(f.col('issuer_tax_id'), r'[^\d\*]', ''))\
    .withColumn('recipient_cnpj', f.regexp_replace(f.col('recipient_cnpj'), r'[^\d\*]', ''))


# Uppercasing names
nfe_df = nfe_df\
    .withColumn('issuer_corporate_name', f.upper(f.col('issuer_corporate_name')))\
    .withColumn('recipient_name', f.upper(f.col('recipient_name')))

item_df = item_df\
    .withColumn('issuer_corporate_name', f.upper(f.col('issuer_corporate_name')))\
    .withColumn('recipient_name', f.upper(f.col('recipient_name')))


# Parsing monetary values
def brl_string_to_monetary(col: Column) -> Column:
    col = f.regexp_replace(col, r'\.', '')
    col = f.regexp_replace(col, r',', '.')
    col = col.cast(FloatType()) * 100
    col = f.round(col, 0).cast(IntegerType())
    return col


nfe_df = nfe_df\
    .withColumn('value', brl_string_to_monetary(f.col('value')))

item_df = item_df\
    .withColumn('unit_value', brl_string_to_monetary(f.col('unit_value')))\
    .withColumn('total_value', brl_string_to_monetary(f.col('total_value')))


# Parsing quantity values
def brl_string_to_float(col: Column) -> Column:
    col = f.regexp_replace(col, r'\.', '')
    col = f.regexp_replace(col, r',', '.')
    col = col.cast(FloatType())
    return col


item_df = item_df\
    .withColumn('quantity', brl_string_to_float(f.col('quantity')))


# Creating an unique ID
@f.udf(StringType())
def generate_id(*args: List[str]) -> str:
    name = ':'.join([str(a) for a in args])

    id = uuid.uuid5(uuid.NAMESPACE_OID, name)
    return str(id)


id_components = [
    'access_key',
    'model',
    'series',
    'number',
    'operation_nature',
    'issue_date',
]

nfe_df = nfe_df.withColumn('nfe_id', generate_id(*id_components))
event_df = event_df.withColumn('nfe_id', generate_id(*id_components))
item_df = item_df.withColumn('nfe_id', generate_id(*id_components))


# Group data
event_df = (
    event_df
    .select(
        'nfe_id',
        f.struct(*[
            col
            for col in event_df.columns
            if col not in (id_components + ['nfe_id'])
        ]).alias('event'),
    )
    .groupBy('nfe_id')
    .agg(f.collect_list('event').alias('events'))
)

item_df = (
    item_df
    .select(
        'nfe_id',
        f.struct(*[
            col
            for col in item_df.columns
            if col not in (id_components + ['nfe_id'])
        ]).alias('item')
    )
    .groupBy('nfe_id')
    .agg(f.collect_list('item').alias('items'))
)


# Join datasets
nfe_df = nfe_df\
    .join(event_df, 'nfe_id', 'left')\
    .join(item_df, 'nfe_id', 'left')


# Saving to ElasticSearch
index_name = str(spark.conf.get('spark.es.resource'))

nfe_df.write.format('org.elasticsearch.spark.sql')\
    .mode('append')\
    .option('es.resource', index_name)\
    .option('es.write.operation', 'index')\
    .option('es.mapping.id', 'nfe_id')\
    .option('es.mapping.exclude', 'nfe_id')\
    .save()
