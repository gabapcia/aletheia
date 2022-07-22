import uuid
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType


spark = SparkSession.builder.appName('RFB CNPJ - Partners').getOrCreate()


# Configs
PARTNER_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.partner'))
COUNTRY_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.country'))
PARTNER_QUALIFICATION_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.partner_qualification'))


# Schemas
schema = StructType([
    StructField('base_cnpj', StringType(), nullable=False),
    StructField('type_code', IntegerType(), nullable=False),
    StructField('name', StringType(), nullable=False),
    StructField('tax_id', StringType(), nullable=False),
    StructField('qualification_code', IntegerType(), nullable=False),
    StructField('join_date', DateType(), nullable=False),
    StructField('country_code', IntegerType(), nullable=False),
    StructField('legal_representative_tax_id', StringType(), nullable=True),
    StructField('legal_representative_name', StringType(), nullable=True),
    StructField('legal_representative_qualification_code', IntegerType(), nullable=True),
    StructField('age_group_code', IntegerType(), nullable=True),
])

code_schema = StructType([
    StructField('key', IntegerType(), nullable=False),
    StructField('value', StringType(), nullable=False),
])


# Enums
partner_type_df = spark.createDataFrame(
    data={
        1: 'PESSOA JURÍDICA',
        2: 'PESSOA FÍSICA',
        3: 'ESTRANGEIRO',
    }.items(),
    schema=code_schema,
)

age_group_df = spark.createDataFrame(
    data={
        0: 'N/A',
        1: '0-12 ANOS',
        2: '13-20 ANOS',
        3: '21-30 ANOS',
        4: '31-40 ANOS',
        5: '41-50 ANOS',
        6: '51-60 ANOS',
        7: '61-70 ANOS',
        8: '71-80 ANOS',
        9: '80+ ANOS',
    }.items(),
    schema=code_schema,
)

country_df = spark.read\
    .schema(code_schema)\
    .option('header', 'false')\
    .option('delimiter', ';')\
    .option('quote', '"')\
    .option('encoding', 'ISO-8859-1')\
    .csv(f's3a://{COUNTRY_BUCKET_PATH}')\
    .withColumn('value', f.upper('value'))

qualification_df = spark.read\
    .schema(code_schema)\
    .option('header', 'false')\
    .option('delimiter', ';')\
    .option('quote', '"')\
    .option('encoding', 'ISO-8859-1')\
    .csv(f's3a://{PARTNER_QUALIFICATION_BUCKET_PATH}')\
    .withColumn('value', f.upper('value'))


# Loading the dataset
partner_df = spark.read\
    .schema(schema)\
    .options(
        header='false',
        delimiter=';',
        quote='"',
        encoding='ISO-8859-1',
        dateFormat='yyyyMMdd',
    )\
    .csv(f's3a://{PARTNER_BUCKET_PATH}')


# Formatting tax ids
partner_df = partner_df\
    .withColumn('tax_id', f.regexp_replace(f.col('tax_id'), r'[^\d\*]', ''))\
    .withColumn('legal_representative_tax_id', f.regexp_replace(f.col('legal_representative_tax_id'), r'[^\d\*]', ''))


# Resolving enumerations
partner_df = (
    partner_df

    .join(partner_type_df, partner_df.type_code == partner_type_df.key, 'left')
    .withColumnRenamed('value', 'type')
    .drop('key')

    .join(age_group_df, partner_df.age_group_code == age_group_df.key, 'left')
    .withColumnRenamed('value', 'age_group')
    .drop('key')

    .join(qualification_df, partner_df.qualification_code == qualification_df.key, 'left')
    .withColumnRenamed('value', 'qualification')
    .drop('key')

    .join(qualification_df, partner_df.legal_representative_qualification_code == qualification_df.key, 'left')
    .withColumnRenamed('value', 'legal_representative_qualification')
    .drop('key')

    .join(country_df, partner_df.country_code == country_df.key, 'left')
    .withColumnRenamed('value', 'country')
    .drop('key')
)


# Uppercasing names
partner_df = partner_df\
    .withColumn('name', f.upper(f.col('name')))\
    .withColumn('legal_representative_name', f.upper(f.col('legal_representative_name')))


# Creating an unique ID
@f.udf(StringType())
def generate_id(base_cnpj: str, type_code: int, name: str, tax_id: str) -> str:
    id = uuid.uuid5(uuid.NAMESPACE_OID, f'{base_cnpj}:{type_code}:{name}:{tax_id}')
    return str(id)


partner_df = partner_df\
    .withColumn('partner_id', generate_id('base_cnpj', 'type_code', 'name', 'tax_id'))


# Grouping the "Legal Representative" fields
valid_lr = f.col('legal_representative_tax_id') != '***000000**'

partner_df = (
    partner_df

    .withColumn('legal_representative', f.struct(
        f.when(valid_lr, f.col('legal_representative_name')).alias('name'),
        f.when(valid_lr, f.col('legal_representative_qualification_code')).alias('qualification_code'),
        f.when(valid_lr, f.col('legal_representative_qualification')).alias('qualification'),
        f.when(valid_lr, f.col('legal_representative_tax_id')).alias('tax_id'),
    ))

    .drop(
        'legal_representative_name',
        'legal_representative_qualification_code',
        'legal_representative_qualification',
        'legal_representative_tax_id',
    )
)


# Saving to ElasticSearch
index_name = str(spark.conf.get('spark.es.resource'))

partner_df.write.format('org.elasticsearch.spark.sql')\
    .mode('append')\
    .option('es.resource', index_name)\
    .option('es.write.operation', 'index')\
    .option('es.mapping.id', 'partner_id')\
    .option('es.mapping.exclude', 'partner_id')\
    .save()
