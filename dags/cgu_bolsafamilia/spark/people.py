from typing import List
import uuid
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import Column
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


spark = SparkSession.builder.appName('CGU BOLSA FAMILIA - People').getOrCreate()


# Configs
PAYMENT_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.payment'))
WITHDRAW_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.withdraw'))


# Schemas
payment_schema = StructType([
    StructField('reference_date', StringType(), nullable=False),
    StructField('competency_date', StringType(), nullable=False),
    StructField('federative_unit', StringType(), nullable=False),
    StructField('county_siafi_code', StringType(), nullable=False),
    StructField('county_siafi', StringType(), nullable=False),
    StructField('tax_id', StringType(), nullable=False),
    StructField('nis', StringType(), nullable=False),
    StructField('name', StringType(), nullable=False),
    StructField('value', StringType(), nullable=False),
])

withdraw_schema = StructType([
    StructField('reference_date', StringType(), nullable=False),
    StructField('competency_date', StringType(), nullable=False),
    StructField('federative_unit', StringType(), nullable=False),
    StructField('county_siafi_code', StringType(), nullable=False),
    StructField('county_siafi', StringType(), nullable=False),
    StructField('tax_id', StringType(), nullable=False),
    StructField('nis', StringType(), nullable=False),
    StructField('name', StringType(), nullable=False),
    StructField('date', StringType(), nullable=False),
    StructField('value', StringType(), nullable=False),
])


# Loading the dataset
payment_df = spark.read\
    .schema(payment_schema)\
    .options(
        header='true',
        delimiter=';',
        quote='"',
        encoding='ISO-8859-1',
    )\
    .csv(f's3a://{PAYMENT_BUCKET_PATH}')

withraw_df = spark.read\
    .schema(withdraw_schema)\
    .options(
        header='true',
        delimiter=';',
        quote='"',
        encoding='ISO-8859-1',
    )\
    .csv(f's3a://{WITHDRAW_BUCKET_PATH}')


# Uppercasing names
payment_df = payment_df\
    .withColumn('name', f.upper(f.col('name')))

withraw_df = withraw_df\
    .withColumn('name', f.upper(f.col('name')))


# Formatting tax ids
payment_df = payment_df\
    .withColumn('tax_id', f.regexp_replace(f.col('tax_id'), r'[^\d\*]', ''))

withraw_df = withraw_df\
    .withColumn('tax_id', f.regexp_replace(f.col('tax_id'), r'[^\d\*]', ''))


# Parsing dates
payment_df = payment_df\
    .withColumn('reference_date', f.to_date(f.col('reference_date'), 'yyyyMM'))\
    .withColumn('competency_date', f.to_date(f.col('competency_date'), 'yyyyMM'))

withraw_df = withraw_df\
    .withColumn('date', f.to_date(f.col('date'), 'dd/MM/yyyy'))\
    .withColumn('reference_date', f.to_date(f.col('reference_date'), 'yyyyMM'))\
    .withColumn('competency_date', f.to_date(f.col('competency_date'), 'yyyyMM'))


# Parsing monetary values
def brl_string_to_monetary(col: Column) -> Column:
    col = f.regexp_replace(col, r'\.', '')
    col = f.regexp_replace(col, r',', '.')
    col = col.cast(FloatType()) * 100
    col = f.round(col, 0).cast(IntegerType())
    return col


payment_df = payment_df\
    .withColumn('value', brl_string_to_monetary(f.col('value')))

withraw_df = withraw_df\
    .withColumn('value', brl_string_to_monetary(f.col('value')))


# Creating an unique ID
@f.udf(StringType())
def generate_id(*args: List[str]) -> str:
    name = ':'.join([str(a) for a in args])

    id = uuid.uuid5(uuid.NAMESPACE_OID, name)
    return str(id)


payment_id_fields = [
    'reference_date',
    'competency_date',
    'federative_unit',
    'county_siafi_code',
    'tax_id',
    'nis',
    'name',
]

payment_df = payment_df\
    .withColumn('payment_id', generate_id(*payment_id_fields))

withraw_df = withraw_df\
    .withColumn('payment_id', generate_id(*payment_id_fields))


# Joining datasets
withraw_agg = (
    withraw_df
    .select('payment_id', f.struct('date', 'value').alias('withdraw'))
    .groupBy('payment_id')
    .agg(f.collect_list('withdraw').alias('withdraws'))
)

payment_df = payment_df.join(withraw_agg, 'payment_id', 'left')

# Saving to ElasticSearch
index_name = str(spark.conf.get('spark.es.resource'))

payment_df.write.format('org.elasticsearch.spark.sql')\
    .mode('append')\
    .option('es.resource', index_name)\
    .option('es.write.operation', 'index')\
    .option('es.mapping.id', 'payment_id')\
    .option('es.mapping.exclude', 'payment_id')\
    .save()
