from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType


spark = SparkSession.builder.appName('RFB CNPJ - Partners').getOrCreate()


# Schemas
schema = StructType([
    StructField(name='base_cnpj', dataType=StringType(), nullable=False),
    StructField(name='type_code', dataType=IntegerType(), nullable=False),
    StructField(name='name', dataType=StringType(), nullable=False),
    StructField(name='tax_id', dataType=StringType(), nullable=False),
    StructField(name='qualification_code', dataType=StringType(), nullable=False),
    StructField(name='join_date', dataType=DateType(), nullable=False),
    StructField(name='country_code', dataType=StringType(), nullable=False),
    StructField(name='legal_representative_tax_id', dataType=StringType(), nullable=True),
    StructField(name='legal_representative_name', dataType=StringType(), nullable=True),
    StructField(name='legal_representative_qualification_code', dataType=StringType(), nullable=True),
    StructField(name='age_group_code', dataType=IntegerType(), nullable=True),
])

code_schema = StructType([
    StructField(name='key', dataType=StringType(), nullable=False),
    StructField(name='value', dataType=StringType(), nullable=False),
])


# Enums
TYPE = spark.createDataFrame(
    data={
        1: 'PJ',
        2: 'PF',
        3: 'FOREIGN',
    }.items(),
    schema=StructType(fields=[
        StructField(name='key', dataType=IntegerType(), nullable=False),
        StructField(name='value', dataType=StringType(), nullable=False),
    ]),
)

AGE_GROUP = spark.createDataFrame(
    data={
        0: None,
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
    schema=StructType(fields=[
        StructField(name='key', dataType=IntegerType(), nullable=False),
        StructField(name='value', dataType=StringType(), nullable=True),
    ]),
)

COUNTRY = spark.read\
    .schema(code_schema)\
    .option('header', 'false')\
    .option('delimiter', ';')\
    .option('quote', '"')\
    .option('encoding', 'ISO-8859-1')\
    .csv('s3a://rfbcnpj/extracted/single/country_map')\
    .withColumn('value', f.upper('value'))\
    .withColumn('key', f.col('key').cast(IntegerType()))

QUALIFICATION = spark.read\
    .schema(code_schema)\
    .option('header', 'false')\
    .option('delimiter', ';')\
    .option('quote', '"')\
    .option('encoding', 'ISO-8859-1')\
    .csv('s3a://rfbcnpj/extracted/single/partner_qualification_map')\
    .withColumn('value', f.upper('value'))\
    .withColumn('key', f.col('key').cast(IntegerType()))


# Loading the dataset
partner_df = (
    spark.read
    .schema(schema)
    .options(
        header='false',
        delimiter=';',
        quote='"',
        encoding='ISO-8859-1',
        dateFormat='yyyyMMdd',
    )
    .csv('s3a://rfbcnpj/extracted/partner/*')
)


# Formatting tax ids
partner_df = partner_df\
    .withColumn('tax_id', f.regexp_replace(f.col('tax_id'), r'[^\d\*]', ''))\
    .withColumn('legal_representative_tax_id', f.regexp_replace(f.col('legal_representative_tax_id'), r'[^\d\*]', ''))


# Cleaning the "Legal Representative" fields
valid_legal_representative = f.col('legal_representative_tax_id') != '***000000**'

partner_df = (
    partner_df

    .withColumn(
        'legal_representative_name',
        f.when(valid_legal_representative, f.col('legal_representative_name')),
    )

    .withColumn(
        'legal_representative_qualification_code',
        f.when(valid_legal_representative, f.col('legal_representative_qualification_code')),
    )

    .withColumn(
        'legal_representative_tax_id',
        f.when(valid_legal_representative, f.col('legal_representative_tax_id')),
    )
)


# Resolving enumerations
partner_df = (
    partner_df

    .join(TYPE, partner_df.type_code == TYPE.key, 'left')
        .withColumnRenamed('value', 'type')
        .drop('key')

    .join(AGE_GROUP, partner_df.age_group_code == AGE_GROUP.key, 'left')
        .withColumnRenamed('value', 'age_group')
        .drop('key')

    .join(QUALIFICATION, partner_df.qualification_code == QUALIFICATION.key, 'left')
        .withColumnRenamed('value', 'qualification')
        .drop('key')

    .join(QUALIFICATION, partner_df.legal_representative_qualification_code == QUALIFICATION.key, 'left')
        .withColumnRenamed('value', 'legal_representative_qualification')
        .drop('key')

    .join(COUNTRY, partner_df.country_code == COUNTRY.key, 'left')
        .withColumnRenamed('value', 'country')
        .drop('key')
)


# Uppercasing names
partner_df = partner_df\
    .withColumn('name', f.upper(f.col('name')))\
    .withColumn('legal_representative_name', f.upper(f.col('legal_representative_name')))


# Saving to Cassandra
partner_df.fillna('').write.format('org.apache.spark.sql.cassandra')\
    .mode('append')\
    .options(keyspace='rfb_cnpj', table='partner')\
    .save()
