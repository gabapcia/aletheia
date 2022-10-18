from typing import Dict
from elasticsearch.exceptions import RequestError as ElasticsearchRequestError
from airflow.decorators import task
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook
from cgu_cpgf.operators.file_storage import FILEDATE_KEY


ELASTICSEARCH_CONN_ID = 'elasticsearch_default'
INDEX_KEY = 'index'


@task(multiple_outputs=False)
def elasticsearch(file_data: Dict[str, str]) -> Dict[str, str]:
    index_name = f'cgu-cpgf-{file_data[FILEDATE_KEY]}'
    transaction_index_conf = {
        'settings': {
            'index.mapping.coerce': False,
            'number_of_shards': 1,
            'number_of_replicas': 0,
        },
        'mappings': {
            'properties': {
                'higher_agency_code': {'type': 'keyword'},
                'higher_agency_name': {'type': 'text', 'index': False},
                'agency_code': {'type': 'keyword'},
                'agency_name': {'type': 'text', 'index': False},
                'management_unit_code': {'type': 'keyword'},
                'management_unit_name': {'type': 'text', 'index': False},
                'statement_year_month': {'type': 'keyword'},
                'card_holder_cpf': {'type': 'keyword'},
                'card_holder_name': {'type': 'text'},
                'beneficiary_tax_id': {'type': 'keyword'},
                'beneficiary_name': {'type': 'text'},
                'transaction': {'type': 'text', 'index': False},
                'transaction_date': {'type': 'date'},
                'transaction_value': {'type': 'long'},
            },
        },
    }

    es = ElasticsearchHook(elasticsearch_conn_id=ELASTICSEARCH_CONN_ID)
    with es.get_conn() as conn:
        try:
            conn.es.indices.create(index_name, body=transaction_index_conf)
        except ElasticsearchRequestError as e:
            if e.error != 'resource_already_exists_exception':
                raise

    file_data[INDEX_KEY] = index_name

    return file_data
