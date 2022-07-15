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
COUNTRY_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.country'))
COUNTY_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.county'))
REGISTER_SITUATION_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.register_situation'))
CNAE_INFO_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.cnae_info'))
LEGAL_NATURE_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.legal_nature'))
PARTNER_QUALIFICATION_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.partner_qualification'))
SIMPLES_INFO_BUCKET_PATH = str(spark.conf.get('spark.aletheia.buckets.simples'))


# region Parsing the Branch files

# Schemas
schema = StructType([
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

cnae_schema = StructType([
    StructField('cnae_number', StringType(), nullable=False),
    StructField('description', StringType(), nullable=False),
])

code_schema = StructType(fields=[
    StructField('key', IntegerType(), nullable=False),
    StructField('value', StringType(), nullable=False),
])

# Enums
company_type_df = spark.createDataFrame(
    data={
        1: 'MATRIZ',
        2: 'FILIAL',
    }.items(),
    schema=code_schema,
)

situation_df = spark.createDataFrame(
    data={
        1: 'NULA',
        2: 'ATIVA',
        3: 'SUSPENSA',
        4: 'INAPTA',
        8: 'BAIXADA',
    }.items(),
    schema=code_schema,
)

reason_situation_df = spark.read\
    .schema(code_schema)\
    .option('header', 'false')\
    .option('delimiter', ';')\
    .option('quote', '"')\
    .option('encoding', 'ISO-8859-1')\
    .csv(f's3a://{REGISTER_SITUATION_BUCKET_PATH}')\
    .withColumn('value', f.upper('value'))

country_df = spark.read\
    .schema(code_schema)\
    .option('header', 'false')\
    .option('delimiter', ';')\
    .option('quote', '"')\
    .option('encoding', 'ISO-8859-1')\
    .csv(f's3a://{COUNTRY_BUCKET_PATH}')\
    .withColumn('value', f.upper('value'))

county_df = spark.read\
    .schema(code_schema)\
    .option('header', 'false')\
    .option('delimiter', ';')\
    .option('quote', '"')\
    .option('encoding', 'ISO-8859-1')\
    .csv(f's3a://{COUNTY_BUCKET_PATH}')\
    .withColumn('value', f.upper('value'))


# Loading the dataset
branch_df = (
    spark.read
    .schema(schema)
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

cnae_data = (
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

    .join(cnae_data, f.col('cnae') == cnae_data.cnae_number, 'left')
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

    .join(cnae_data, branch_df.cnae == cnae_data.cnae_number, 'left')
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


# Uppercasing names
branch_df = branch_df\
    .withColumn('trading_name', f.upper(f.col('trading_name')))


# endregion


# region Parsing the Company files

# Schemas
schema = StructType([
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

code_schema = StructType([
    StructField('key', IntegerType(), nullable=False),
    StructField('value', StringType(), nullable=False),
])


# Enums
size_df = spark.createDataFrame(
    data={
        0: 'NÃO INFORMADO',
        1: 'MICRO EMPRESA',
        3: 'EMPRESA DE PEQUENO PORTE',
        5: 'DEMAIS',
    }.items(),
    schema=code_schema,
)

legal_nature_df = spark.read\
    .schema(code_schema)\
    .option('header', 'false')\
    .option('delimiter', ';')\
    .option('quote', '"')\
    .option('encoding', 'ISO-8859-1')\
    .csv(f's3a://{LEGAL_NATURE_BUCKET_PATH}')\
    .withColumn('value', f.upper('value'))

qualification_df = spark.read\
    .schema(code_schema)\
    .option('header', 'false')\
    .option('delimiter', ';')\
    .option('quote', '"')\
    .option('encoding', 'ISO-8859-1')\
    .csv(f's3a://{PARTNER_QUALIFICATION_BUCKET_PATH}')\
    .withColumn('value', f.upper('value'))


# Loading the datasets
company_df = (
    spark.read
    .schema(schema)
    .options(
        header='false',
        delimiter=';',
        quote='"',
        encoding='ISO-8859-1',
    )
    .csv(f's3a://{COMPANY_BUCKET_PATH}')
)

simples_data = (
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
)


# Parsing SIMPLES dates
simples_data = (
    simples_data

    .withColumn(
        'date_opted_for_simples',
        f.when(f.col('date_opted_for_simples') == '00000000', None)
        .otherwise(f.to_date('date_opted_for_simples', 'yyyyMMdd'))
    )

    .withColumn(
        'simples_exclusion_date',
        f.when(f.col('simples_exclusion_date') == '00000000', None)
        .otherwise(f.to_date('simples_exclusion_date', 'yyyyMMdd'))
    )

    .withColumn(
        'date_opted_for_mei',
        f.when(f.col('date_opted_for_mei') == '00000000', None)
        .otherwise(f.to_date('date_opted_for_mei', 'yyyyMMdd'))
    )

    .withColumn(
        'mei_exclusion_date',
        f.when(f.col('mei_exclusion_date') == '00000000', None)
        .otherwise(f.to_date('mei_exclusion_date', 'yyyyMMdd'))
    )
)


# Merging datasets
company_df = company_df.join(simples_data, 'base_cnpj', 'left')


# Resolve enumerations
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
company_df = company_df\
    .withColumn('corporate_name', f.upper(f.col('corporate_name')))


# endregion


data = branch_df.join(company_df, 'base_cnpj', 'left')


# Saving to ElasticSearch
index_name = str(spark.conf.get('spark.es.resource'))

data.write.format('org.elasticsearch.spark.sql')\
    .mode('append')\
    .option('es.resource', index_name)\
    .option('es.write.operation', 'index')\
    .option('es.mapping.id', 'cnpj')\
    .save()
