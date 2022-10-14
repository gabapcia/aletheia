import uuid
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType
from pyspark.sql import Column


spark = SparkSession.builder\
    .config('spark.sql.mapKeyDedupPolicy', 'LAST_WIN')\
    .appName('RFB CNPJ - Company')\
    .getOrCreate()


# Configs
COMPANY_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.company'))
BRANCH_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.branch'))
PARTNER_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.partner'))
COUNTRY_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.country'))
COUNTY_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.county'))
REGISTER_SITUATION_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.register_situation'))
CNAE_INFO_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.cnae_info'))
LEGAL_NATURE_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.legal_nature_info'))
PARTNER_QUALIFICATION_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.partner_qualification'))
SIMPLES_INFO_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.simples_info'))


# Schemas
branch_schema = StructType([
    StructField('base_cnpj', StringType(), nullable=False),
    StructField('order_cnpj', StringType(), nullable=False),
    StructField('vd_cnpj', StringType(), nullable=False),
    StructField('type_code', IntegerType(), nullable=False),
    StructField('trading_name', StringType(), nullable=False),
    StructField('situation_code', IntegerType(), nullable=False),
    StructField('date_situation', DateType(), nullable=False),
    StructField('reason_situation_code', IntegerType(), nullable=False),
    StructField('foreign_city_name', StringType(), nullable=False),
    StructField('country_code', IntegerType(), nullable=False),
    StructField('start_date', DateType(), nullable=False),
    StructField('cnae', StringType(), nullable=False),
    StructField('other_cnaes', StringType(), nullable=False),
    StructField('address_type', StringType(), nullable=False),
    StructField('address', StringType(), nullable=False),
    StructField('number', StringType(), nullable=False),
    StructField('complement', StringType(), nullable=False),
    StructField('district', StringType(), nullable=False),
    StructField('zip_code', StringType(), nullable=False),
    StructField('federative_unit', StringType(), nullable=False),
    StructField('county_code', IntegerType(), nullable=False),
    StructField('ddd_1', StringType(), nullable=False),
    StructField('phone_1', StringType(), nullable=False),
    StructField('ddd_2', StringType(), nullable=False),
    StructField('phone_2', StringType(), nullable=False),
    StructField('ddd_fax', StringType(), nullable=False),
    StructField('fax', StringType(), nullable=False),
    StructField('email', StringType(), nullable=False),
    StructField('special_situation', StringType(), nullable=False),
    StructField('date_special_situation', DateType(), nullable=False),
])

company_schema = StructType([
    StructField('base_cnpj', StringType(), nullable=False),
    StructField('corporate_name', StringType(), nullable=False),
    StructField('legal_nature_code', IntegerType(), nullable=False),
    StructField('responsible_qualification_code', IntegerType(), nullable=False),
    StructField('share_capital', StringType(), nullable=False),
    StructField('size_code', IntegerType(), nullable=False),
    StructField('responsible_federative_entity', StringType(), nullable=False),
])

simples_schema = StructType([
    StructField('base_cnpj', StringType(), nullable=False),
    StructField('opted_for_simples', StringType(), nullable=False),
    StructField('date_opted_for_simples', StringType(), nullable=False),
    StructField('simples_exclusion_date', StringType(), nullable=False),
    StructField('opted_for_mei', StringType(), nullable=False),
    StructField('date_opted_for_mei', StringType(), nullable=False),
    StructField('mei_exclusion_date', StringType(), nullable=False),
])

cnae_schema = StructType([
    StructField('cnae_number', StringType(), nullable=False),
    StructField('description', StringType(), nullable=False),
])

code_schema = StructType([
    StructField('key', IntegerType(), nullable=False),
    StructField('value', StringType(), nullable=False),
])


# Enums
company_type_df = f.broadcast(spark.createDataFrame(
    data={
        1: 'MATRIZ',
        2: 'FILIAL',
    }.items(),
    schema=code_schema,
))

situation_df = f.broadcast(spark.createDataFrame(
    data={
        1: 'NULA',
        2: 'ATIVA',
        3: 'SUSPENSA',
        4: 'INAPTA',
        8: 'BAIXADA',
    }.items(),
    schema=code_schema,
))

reason_situation_df = f.broadcast(
    spark.read
        .schema(code_schema)
        .option('header', 'false')
        .option('delimiter', ';')
        .option('quote', '"')
        .option('encoding', 'ISO-8859-1')
        .csv(f's3a://{REGISTER_SITUATION_BUCKET_PATH}')
        .withColumn('value', f.upper('value'))
)

country_df = f.broadcast(
    spark.read
        .schema(code_schema)
        .option('header', 'false')
        .option('delimiter', ';')
        .option('quote', '"')
        .option('encoding', 'ISO-8859-1')
        .csv(f's3a://{COUNTRY_BUCKET_PATH}')
        .withColumn('value', f.upper('value'))
)

county_df = f.broadcast(
    spark.read
        .schema(code_schema)
        .option('header', 'false')
        .option('delimiter', ';')
        .option('quote', '"')
        .option('encoding', 'ISO-8859-1')
        .csv(f's3a://{COUNTY_BUCKET_PATH}')
        .withColumn('value', f.upper('value'))
)

size_df = f.broadcast(spark.createDataFrame(
    data={
        0: 'NÃO INFORMADO',
        1: 'MICRO EMPRESA',
        3: 'EMPRESA DE PEQUENO PORTE',
        5: 'DEMAIS',
    }.items(),
    schema=code_schema,
))

legal_nature_df = f.broadcast(
    spark.read
        .schema(code_schema)
        .option('header', 'false')
        .option('delimiter', ';')
        .option('quote', '"')
        .option('encoding', 'ISO-8859-1')
        .csv(f's3a://{LEGAL_NATURE_BUCKET_PATH}')
        .withColumn('value', f.upper('value'))
)

qualification_df = f.broadcast(
    spark.read
        .schema(code_schema)
        .option('header', 'false')
        .option('delimiter', ';')
        .option('quote', '"')
        .option('encoding', 'ISO-8859-1')
        .csv(f's3a://{PARTNER_QUALIFICATION_BUCKET_PATH}')
        .withColumn('value', f.upper('value'))
)


# Datasets
company_df = (
    spark.read
    .schema(company_schema)
    .options(
        header='false',
        delimiter=';',
        quote='"',
        encoding='ISO-8859-1',
    )
    .csv(f's3a://{COMPANY_BUCKET_PATH}')
)

branch_df = (
    spark.read
    .schema(branch_schema)
    .options(
        header='false',
        delimiter=';',
        quote='"',
        encoding='ISO-8859-1',
        dateFormat='yyyyMMdd',
        # Broken record
        escape='',
        unescapedQuoteHandling='STOP_AT_CLOSING_QUOTE',
    )
    .csv(f's3a://{BRANCH_BUCKET_PATH}')
)

simples_df = (
    spark.read
    .schema(simples_schema)
    .options(
        header='false',
        delimiter=';',
        quote='"',
        encoding='ISO-8859-1',
        dateFormat='yyyyMMdd',
    )
    .csv(f's3a://{SIMPLES_INFO_BUCKET_PATH}')
    .withColumn(
        'date_opted_for_simples',
        f.when(f.col('date_opted_for_simples') != '00000000', f.to_date('date_opted_for_simples', 'yyyyMMdd'))
    )
    .withColumn(
        'simples_exclusion_date',
        f.when(f.col('simples_exclusion_date') != '00000000', f.to_date('simples_exclusion_date', 'yyyyMMdd'))
    )
    .withColumn(
        'date_opted_for_mei',
        f.when(f.col('date_opted_for_mei') != '00000000', f.to_date('date_opted_for_mei', 'yyyyMMdd'))
    )
    .withColumn(
        'mei_exclusion_date',
        f.when(f.col('mei_exclusion_date') != '00000000', f.to_date('mei_exclusion_date', 'yyyyMMdd'))
    )
)

cnae_df = (
    spark.read
    .schema(cnae_schema)
    .options(
        header='false',
        delimiter=';',
        quote='"',
        encoding='ISO-8859-1',
    )
    .csv(f's3a://{CNAE_INFO_BUCKET_PATH}')
)


# Join CNPJ
branch_df = branch_df\
    .withColumn('cnpj', f.concat_ws('', f.col('base_cnpj'), f.col('order_cnpj'), f.col('vd_cnpj')))\
    .drop('order_cnpj', 'vd_cnpj')


# Populating CNAEs
other_cnaes = (
    branch_df

    .select(
        f.col('cnpj'),
        f.explode(f.split(f.col('other_cnaes'), ',', -1)).alias('cnae')
    )

    .join(cnae_df, f.col('cnae') == cnae_df.cnae_number, 'left')
    .drop('cnae_number')

    .groupBy(f.col('cnpj'))

    .agg(
        f.map_from_arrays(
            f.collect_list('cnae'),
            f.collect_list('description'),
        ).alias('other_cnaes'),
    )
)

branch_df = (
    branch_df

    .drop('other_cnaes')

    .join(other_cnaes, 'cnpj', 'left')

    .join(cnae_df, branch_df.cnae == cnae_df.cnae_number, 'left')
    .withColumnRenamed('description', 'cnae_description')
    .drop('cnae_number')
)


# Parse address
branch_df = branch_df\
    .withColumn('address', f.concat_ws(' ', 'address_type', 'address'))\
    .drop('address_type')


# Parse phone numbers
branch_df = branch_df\
    .withColumn('phone_1', f.concat_ws(' ', 'ddd_1', 'phone_1'))\
    .withColumn('phone_2', f.concat_ws(' ', 'ddd_2', 'phone_2'))\
    .withColumn('fax', f.concat_ws(' ', 'ddd_fax', 'fax'))\
    .drop('ddd_1', 'ddd_2', 'ddd_fax')


# Resolve enumerations
branch_df = (
    branch_df

    .join(company_type_df, branch_df.type_code == company_type_df.key, 'left')
    .withColumnRenamed('value', 'type')
    .drop('key')

    .join(situation_df, branch_df.situation_code == situation_df.key, 'left')
    .withColumnRenamed('value', 'situation')
    .drop('key')

    .join(reason_situation_df, branch_df.reason_situation_code == reason_situation_df.key, 'left')
    .withColumnRenamed('value', 'reason_situation')
    .drop('key')

    .join(country_df, branch_df.country_code == country_df.key, 'left')
    .withColumnRenamed('value', 'country')
    .drop('key')

    .join(county_df, branch_df.county_code == county_df.key, 'left')
    .withColumnRenamed('value', 'county')
    .drop('key')
)

company_df = (
    company_df

    .join(size_df, company_df.size_code == size_df.key, 'left')
    .withColumnRenamed('value', 'size')
    .drop('key')

    .join(legal_nature_df, company_df.legal_nature_code == legal_nature_df.key, 'left')
    .withColumnRenamed('value', 'legal_nature')
    .drop('key')

    .join(qualification_df, company_df.responsible_qualification_code == qualification_df.key, 'left')
    .withColumnRenamed('value', 'responsible_qualification')
    .drop('key')
)


# Parsing monetary values
def brl_string_to_monetary(col: Column) -> Column:
    col = f.regexp_replace(col, r'\.', '')
    col = f.regexp_replace(col, r',', '.')
    col = col.cast(FloatType()) * 100
    col = f.round(col, 0).cast(IntegerType())
    return col


company_df = company_df\
    .withColumn('share_capital', brl_string_to_monetary(f.col('share_capital')))


# Uppercasing names
branch_df = branch_df\
    .withColumn('trading_name', f.upper(f.col('trading_name')))

company_df = company_df\
    .withColumn('corporate_name', f.upper(f.col('corporate_name')))


# region Parsing the Partner files

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
partner_type_df = f.broadcast(spark.createDataFrame(
    data={
        1: 'PESSOA JURÍDICA',
        2: 'PESSOA FÍSICA',
        3: 'ESTRANGEIRO',
    }.items(),
    schema=code_schema,
))

age_group_df = f.broadcast(spark.createDataFrame(
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
))

country_df = f.broadcast(
    spark.read
        .schema(code_schema)
        .option('header', 'false')
        .option('delimiter', ';')
        .option('quote', '"')
        .option('encoding', 'ISO-8859-1')
        .csv(f's3a://{COUNTRY_BUCKET_PATH}')
        .withColumn('value', f.upper('value'))
)

qualification_df = f.broadcast(
    spark.read
        .schema(code_schema)
        .option('header', 'false')
        .option('delimiter', ';')
        .option('quote', '"')
        .option('encoding', 'ISO-8859-1')
        .csv(f's3a://{PARTNER_QUALIFICATION_BUCKET_PATH}')
        .withColumn('value', f.upper('value'))
)


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


# Cleaning the dataset
partner_df = partner_df\
    .dropDuplicates(subset=['partner_id'])\
    .drop('partner_id')


# Group partners
partner_df = (
    partner_df
    .select(
        'base_cnpj',
        f.struct(*[col for col in partner_df.columns if col != 'base_cnpj']).alias('partner')
    )
    .groupBy('base_cnpj')
    .agg(f.collect_list('partner').alias('partners'))
)

# endregion


# Join datasets
company_df = company_df.join(simples_df, 'base_cnpj', 'left')

data = (
    branch_df
    .join(company_df, 'base_cnpj', 'left')
    .join(partner_df, 'base_cnpj', 'left')
)


# Saving to ElasticSearch
index_name = str(spark.conf.get('spark.es.resource'))

data.write.format('org.elasticsearch.spark.sql')\
    .mode('append')\
    .option('es.resource', index_name)\
    .option('es.write.operation', 'index')\
    .option('es.mapping.id', 'cnpj')\
    .save()
