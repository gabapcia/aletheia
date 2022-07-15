from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook


COMPANIES_INDEX_KEY = 'companies'
PARTNERS_INDEX_KEY = 'partners'


@task
def companies_index() -> str:
    index_name = 'rfb-cnpj-companies'
    company_index_conf = {
        'settings': {
            'index.mapping.coerce': False,
        },
        'mappings': {
            'properties': {
                'base_cnpj': {'type': 'keyword'},
                'cnpj': {'type': 'keyword'},
                'trading_name': {'type': 'text'},
                'corporate_name': {'type': 'text'},
                'email': {'type': 'keyword'},
                'type_code': {'type': 'integer', 'index': False},
                'type': {'type': 'keyword', 'index': False},
                'situation_code': {'type': 'integer', 'index': False},
                'situation': {'type': 'keyword', 'index': False},
                'date_situation': {'type': 'date', 'index': False},
                'reason_situation_code': {'type': 'integer', 'index': False},
                'reason_situation': {'type': 'keyword', 'index': False},
                'start_date': {'type': 'date', 'index': False},
                'cnae': {'type': 'keyword', 'index': False},
                'cnae_description': {'type': 'keyword', 'index': False},
                'other_cnaes': {'type': 'flattened', 'index': False},
                'address': {'type': 'text', 'index': False},
                'number': {'type': 'text', 'index': False},
                'complement': {'type': 'text', 'index': False},
                'district': {'type': 'text', 'index': False},
                'zip_code': {'type': 'keyword', 'index': False},
                'federative_unit': {'type': 'keyword', 'index': False},
                'county_code': {'type': 'integer', 'index': False},
                'county': {'type': 'keyword', 'index': False},
                'country_code': {'type': 'integer', 'index': False},
                'country': {'type': 'keyword', 'index': False},
                'foreign_city_name': {'type': 'keyword', 'index': False},
                'special_situation': {'type': 'keyword', 'index': False},
                'date_special_situation': {'type': 'date', 'index': False},
                'phone_1': {'type': 'keyword', 'index': False},
                'phone_2': {'type': 'keyword', 'index': False},
                'fax': {'type': 'keyword', 'index': False},
                'legal_nature_code': {'type': 'integer', 'index': False},
                'legal_nature': {'type': 'keyword', 'index': False},
                'share_capital': {'type': 'long', 'index': False},
                'size_code': {'type': 'integer', 'index': False},
                'size': {'type': 'keyword', 'index': False},
                'responsible_qualification_code': {'type': 'integer', 'index': False},
                'responsible_qualification': {'type': 'keyword', 'index': False},
                'responsible_federative_entity': {'type': 'keyword', 'index': False},
                'opted_for_simples': {'type': 'keyword', 'index': False},
                'date_opted_for_simples': {'type': 'date', 'index': False},
                'simples_exclusion_date': {'type': 'date', 'index': False},
                'opted_for_mei': {'type': 'keyword', 'index': False},
                'date_opted_for_mei': {'type': 'date', 'index': False},
                'mei_exclusion_date': {'type': 'date', 'index': False},
            }
        }
    }

    es = ElasticsearchHook(elasticsearch_conn_id='elasticsearch_default')
    with es.get_conn() as conn:
        if not conn.es.indices.exists(index=index_name):
            conn.es.indices.create(index_name, body=company_index_conf)

    return index_name


@task
def partners_index() -> str:
    index_name = 'rfb-cnpj-partners'
    company_index_conf = {
        'settings': {
            'index.mapping.coerce': False,
        },
        'mappings': {
            'properties': {
                'base_cnpj': {'type': 'keyword'},
                'type_code': {'type': 'integer', 'index': False},
                'type': {'type': 'keyword', 'index': False},
                'name': {'type': 'text'},
                'tax_id': {'type': 'keyword'},
                'qualification_code': {'type': 'integer', 'index': False},
                'qualification': {'type': 'keyword', 'index': False},
                'join_date': {'type': 'date', 'index': False},
                'age_group_code': {'type': 'integer', 'index': False},
                'age_group': {'type': 'keyword', 'index': False},
                'legal_representative': {
                    'properties': {
                        'name': {'type': 'text'},
                        'tax_id': {'type': 'keyword'},
                        'qualification_code': {'type': 'integer', 'index': False},
                        'qualification': {'type': 'keyword', 'index': False},
                    },
                },
            },
        },
    }

    es = ElasticsearchHook(elasticsearch_conn_id='elasticsearch_default')
    with es.get_conn() as conn:
        if not conn.es.indices.exists(index=index_name):
            conn.es.indices.create(index_name, body=company_index_conf)

    return index_name


def elasticsearch() -> TaskGroup:
    with TaskGroup(group_id='elasticsearch') as elasticsearch:
        companies_index_name = companies_index()
        partners_index_name = partners_index()

    return elasticsearch, {
        COMPANIES_INDEX_KEY: companies_index_name,
        PARTNERS_INDEX_KEY: partners_index_name,
    }
