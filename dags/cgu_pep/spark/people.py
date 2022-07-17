from typing import List
import uuid
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, DateType


spark = SparkSession.builder.appName('CGU PEP - People').getOrCreate()


# Configs
PEOPLE_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.people'))


# Schemas
schema = StructType([
    StructField('tax_id', StringType(), nullable=False),
    StructField('name', StringType(), nullable=False),
    StructField('role_initials', StringType(), nullable=False),
    StructField('role_description', StringType(), nullable=False),
    StructField('role_level', StringType(), nullable=False),
    StructField('federal_agency', StringType(), nullable=False),
    StructField('entry_date', DateType(), nullable=False),
    StructField('exit_date', DateType(), nullable=False),
    StructField('grace_period_end_date', DateType(), nullable=False),
])


# Loading the dataset
people_df = spark.read\
    .schema(schema)\
    .options(
        header='true',
        delimiter=';',
        quote='"',
        encoding='ISO-8859-1',
        dateFormat='dd/MM/yyyy',
    )\
    .csv(f's3a://{PEOPLE_BUCKET_PATH}')


# Formatting tax ids
people_df = people_df\
    .withColumn('tax_id', f.regexp_replace(f.col('tax_id'), r'[^\d\*]', ''))


# Uppercasing names
people_df = people_df\
    .withColumn('name', f.upper(f.col('name')))


# Creating an unique ID
@f.udf(StringType())
def generate_id(*args: List[str]) -> str:
    name = ':'.join([str(a) for a in args])

    id = uuid.uuid5(uuid.NAMESPACE_OID, name)
    return str(id)


people_df = people_df\
    .withColumn('pep_id', generate_id(
        'tax_id',
        'name',
        'role_initials',
        'role_level',
        'federal_agency',
        'entry_date',
        'exit_date',
        'grace_period_end_date',
    ))


# Saving to ElasticSearch
index_name = str(spark.conf.get('spark.es.resource'))

people_df.write.format('org.elasticsearch.spark.sql')\
    .mode('overwrite')\
    .option('es.resource', index_name)\
    .option('es.write.operation', 'index')\
    .option('es.mapping.id', 'pep_id')\
    .option('es.mapping.exclude', 'pep_id')\
    .save()
