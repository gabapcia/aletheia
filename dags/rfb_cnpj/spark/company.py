from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType
from pyspark.sql import Column


spark = SparkSession.builder\
    .config('spark.sql.mapKeyDedupPolicy', 'LAST_WIN')\
    .appName('RFB CNPJ - Company')\
    .getOrCreate()


# Schemas
schema = StructType([
    StructField(name='base_cnpj', dataType=StringType(), nullable=False),
    StructField(name='order_cnpj', dataType=StringType(), nullable=False),
    StructField(name='vd_cnpj', dataType=StringType(), nullable=False),
    StructField(name='type_code', dataType=IntegerType(), nullable=False),
    StructField(name='trading_name', dataType=StringType(), nullable=False),
    StructField(name='situation_code', dataType=IntegerType(), nullable=False),
    StructField(name='date_situation', dataType=DateType(), nullable=False),
    StructField(name='reason_situation_code', dataType=StringType(), nullable=False),
    StructField(name='foreign_city_name', dataType=StringType(), nullable=False),
    StructField(name='country_code', dataType=StringType(), nullable=False),
    StructField(name='start_date', dataType=DateType(), nullable=False),
    StructField(name='cnae', dataType=StringType(), nullable=False),
    StructField(name='other_cnaes', dataType=StringType(), nullable=False),
    StructField(name='address_type', dataType=StringType(), nullable=False),
    StructField(name='address', dataType=StringType(), nullable=False),
    StructField(name='number', dataType=StringType(), nullable=False),
    StructField(name='complement', dataType=StringType(), nullable=False),
    StructField(name='district', dataType=StringType(), nullable=False),
    StructField(name='zip_code', dataType=StringType(), nullable=False),
    StructField(name='federative_unit', dataType=StringType(), nullable=False),
    StructField(name='county_code', dataType=StringType(), nullable=False),
    StructField(name='ddd_1', dataType=StringType(), nullable=False),
    StructField(name='phone_1', dataType=StringType(), nullable=False),
    StructField(name='ddd_2', dataType=StringType(), nullable=False),
    StructField(name='phone_2', dataType=StringType(), nullable=False),
    StructField(name='ddd_fax', dataType=StringType(), nullable=False),
    StructField(name='fax', dataType=StringType(), nullable=False),
    StructField(name='email', dataType=StringType(), nullable=False),
    StructField(name='special_situation', dataType=StringType(), nullable=False),
    StructField(name='date_special_situation', dataType=DateType(), nullable=False),
])

cnae_schema = StructType([
    StructField(name='cnae_number', dataType=StringType(), nullable=False),
    StructField(name='description', dataType=StringType(), nullable=False),
])

code_schema = StructType(fields=[
    StructField(name='key', dataType=IntegerType(), nullable=False),
    StructField(name='value', dataType=StringType(), nullable=False),
])

# Enums
TYPE = spark.createDataFrame(
    data={
        1: 'MATRIZ',
        2: 'FILIAL',
    }.items(),
    schema=code_schema,
)

SITUATION = spark.createDataFrame(
    data={
        1: 'NULA',
        2: 'ATIVA',
        3: 'SUSPENSA',
        4: 'INAPTA',
        8: 'BAIXADA',
    }.items(),
    schema=code_schema,
)

REASON_SITUATION = spark.read\
    .schema(code_schema)\
    .option('header', 'false')\
    .option('delimiter', ';')\
    .option('quote', '"')\
    .option('encoding', 'ISO-8859-1')\
    .csv('s3a://rfbcnpj/extracted/single/register_situation_map')\
    .withColumn('value', f.upper('value'))

COUNTRY = spark.read\
    .schema(code_schema)\
    .option('header', 'false')\
    .option('delimiter', ';')\
    .option('quote', '"')\
    .option('encoding', 'ISO-8859-1')\
    .csv('s3a://rfbcnpj/extracted/single/country_map')\
    .withColumn('value', f.upper('value'))

COUNTY = spark.read\
    .schema(code_schema)\
    .option('header', 'false')\
    .option('delimiter', ';')\
    .option('quote', '"')\
    .option('encoding', 'ISO-8859-1')\
    .csv('s3a://rfbcnpj/extracted/single/county_map')\
    .withColumn('value', f.upper('value'))


############ Parsing the Branch files ############

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
    .csv('s3a://rfbcnpj/extracted/branch/*')
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
    .csv('s3a://rfbcnpj/extracted/single/cnae_info_map')
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

    .join(TYPE, branch_df.type_code == TYPE.key, 'left')
        .withColumnRenamed('value', 'type')
        .drop('key')

    .join(SITUATION, branch_df.situation_code == SITUATION.key, 'left')
        .withColumnRenamed('value', 'situation')
        .drop('key')

    .join(REASON_SITUATION, branch_df.reason_situation_code == REASON_SITUATION.key, 'left')
        .withColumnRenamed('value', 'reason_situation')
        .drop('key')

    .join(COUNTRY, branch_df.country_code == COUNTRY.key, 'left')
        .withColumnRenamed('value', 'country')
        .drop('key')

    .join(COUNTY, branch_df.county_code == COUNTY.key, 'left')
        .withColumnRenamed('value', 'county')
        .drop('key')
)


# Uppercasing names
branch_df = branch_df\
    .withColumn('trading_name', f.upper(f.col('trading_name')))


############ Parsing the Company files ############

# Schemas
schema = StructType([
    StructField(name='base_cnpj', dataType=StringType(), nullable=False),
    StructField(name='corporate_name', dataType=StringType(), nullable=False),
    StructField(name='legal_nature_code', dataType=StringType(), nullable=False),
    StructField(name='responsible_qualification_code', dataType=StringType(), nullable=False),
    StructField(name='share_capital', dataType=StringType(), nullable=False),
    StructField(name='size_code', dataType=IntegerType(), nullable=False),
    StructField(name='responsible_federative_entity', dataType=StringType(), nullable=False),
])

simples_schema = StructType([
    StructField(name='base_cnpj', dataType=StringType(), nullable=False),
    StructField(name='opted_for_simples', dataType=StringType(), nullable=False),
    StructField(name='date_opted_for_simples', dataType=StringType(), nullable=False),
    StructField(name='simples_exclusion_date', dataType=StringType(), nullable=False),
    StructField(name='opted_for_mei', dataType=StringType(), nullable=False),
    StructField(name='date_opted_for_mei', dataType=StringType(), nullable=False),
    StructField(name='mei_exclusion_date', dataType=StringType(), nullable=False),
])

code_schema = StructType([
    StructField(name='key', dataType=IntegerType(), nullable=False),
    StructField(name='value', dataType=StringType(), nullable=False),
])


# Enums
SIZE = spark.createDataFrame(
    data={
        0: 'NÃO INFORMADO',
        1: 'MICRO EMPRESA',
        3: 'EMPRESA DE PEQUENO PORTE',
        5: 'DEMAIS',
    }.items(),
    schema=code_schema,
)

LEGAL_NATURE = spark.read\
    .schema(code_schema)\
    .option('header', 'false')\
    .option('delimiter', ';')\
    .option('quote', '"')\
    .option('encoding', 'ISO-8859-1')\
    .csv('s3a://rfbcnpj/extracted/single/legal_nature_info')\
    .withColumn('value', f.upper('value'))

QUALIFICATION = spark.read\
    .schema(code_schema)\
    .option('header', 'false')\
    .option('delimiter', ';')\
    .option('quote', '"')\
    .option('encoding', 'ISO-8859-1')\
    .csv('s3a://rfbcnpj/extracted/single/partner_qualification_map')\
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
    .csv('s3a://rfbcnpj/extracted/company/*')
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
    .csv('s3a://rfbcnpj/extracted/single/simples_info')
)


# Parsing SIMPLES dates
simples_data = simples_data\
    .withColumn(
        'date_opted_for_simples',
        f.when(f.col('date_opted_for_simples') == '00000000', None)\
            .otherwise(f.to_date('date_opted_for_simples', 'yyyyMMdd'))
    )\
    .withColumn(
        'simples_exclusion_date',
        f.when(f.col('simples_exclusion_date') == '00000000', None)\
            .otherwise(f.to_date('simples_exclusion_date', 'yyyyMMdd'))
    )\
    .withColumn(
        'date_opted_for_mei',
        f.when(f.col('date_opted_for_mei') == '00000000', None)\
            .otherwise(f.to_date('date_opted_for_mei', 'yyyyMMdd'))
    )\
    .withColumn(
        'mei_exclusion_date',
        f.when(f.col('mei_exclusion_date') == '00000000', None)\
            .otherwise(f.to_date('mei_exclusion_date', 'yyyyMMdd'))
    )\


# Merging datasets
company_df = company_df.join(simples_data, 'base_cnpj', 'left')


# Resolve enumerations
company_df = (
    company_df

    .join(SIZE, company_df.size_code == SIZE.key, 'left')
        .withColumnRenamed('value', 'size')
        .drop('key')

    .join(LEGAL_NATURE, company_df.legal_nature_code == LEGAL_NATURE.key, 'left')
        .withColumnRenamed('value', 'legal_nature')
        .drop('key')

    .join(QUALIFICATION, company_df.responsible_qualification_code == QUALIFICATION.key, 'left')
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


############ Joining datasets ############

full = branch_df.join(company_df, 'base_cnpj', 'left')


full.fillna('').write.format('org.apache.spark.sql.cassandra')\
    .mode('append')\
    .options(keyspace='rfb_cnpj', table='company')\
    .save()
