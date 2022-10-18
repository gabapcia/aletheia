from typing import Dict
from elasticsearch.exceptions import RequestError as ElasticsearchRequestError
from airflow.decorators import task
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook
from cgu_nfe.operators.file_storage import FILEDATE_KEY


ELASTICSEARCH_CONN_ID = 'elasticsearch_default'
INDEX_KEY = 'index'


@task(multiple_outputs=False)
def elasticsearch(file_data: Dict[str, str]) -> Dict[str, str]:
    index_name = f'cgu-nfe-{file_data[FILEDATE_KEY]}'
    nfe_index_conf = {
        'settings': {
            'index.mapping.coerce': False,
            'number_of_shards': 1,
            'number_of_replicas': 0,
        },
        'mappings': {
            'properties': {
                'access_key': {'type': 'keyword'},
                'model': {'type': 'keyword'},
                'series': {'type': 'keyword'},
                'number': {'type': 'keyword'},
                'operation_nature': {'type': 'keyword'},
                'issue_date': {'type': 'date'},
                'most_recent_event': {'type': 'text'},
                'most_recent_event_date': {'type': 'date'},
                'issuer_tax_id': {'type': 'keyword'},
                'issuer_corporate_name': {'type': 'text'},
                'issuer_state_registration': {'type': 'text'},
                'issuer_federative_unit': {'type': 'keyword'},
                'issuer_county': {'type': 'text'},
                'recipient_cnpj': {'type': 'keyword'},
                'recipient_name': {'type': 'text'},
                'recipient_federative_unit': {'type': 'keyword'},
                'recipient_state_tax': {'type': 'text'},
                'operation_destination': {'type': 'text'},
                'final_costumer': {'type': 'text'},
                'buyer_presence': {'type': 'text'},
                'value': {'type': 'long'},
                'events': {
                    'type': 'nested',
                    'properties': {
                        'event': {'type': 'text'},
                        'date': {'type': 'date'},
                        'description': {'type': 'text'},
                        'reason': {'type': 'text'},
                    },
                },
                'items': {
                    'type': 'nested',
                    'properties': {
                        'issuer_tax_id': {'type': 'keyword'},
                        'issuer_corporate_name': {'type': 'text'},
                        'issuer_state_registration': {'type': 'text'},
                        'issuer_federative_unit': {'type': 'keyword'},
                        'issuer_county': {'type': 'text'},
                        'recipient_cnpj': {'type': 'keyword'},
                        'recipient_name': {'type': 'text'},
                        'recipient_federative_unit': {'type': 'text'},
                        'recipient_state_tax': {'type': 'text'},
                        'operation_destination': {'type': 'text'},
                        'final_costumer': {'type': 'text'},
                        'buyer_presence': {'type': 'text'},
                        'product_number': {'type': 'text'},
                        'product_description': {'type': 'text'},
                        'ncm_sh_code': {'type': 'text'},
                        'ncm_sh': {'type': 'text', 'index': False},
                        'cfop': {'type': 'text'},
                        'quantity': {'type': 'double'},
                        'unit': {'type': 'text'},
                        'unit_value': {'type': 'text'},
                        'total_value': {'type': 'text'},
                    },
                },
            },
        },
    }

    es = ElasticsearchHook(elasticsearch_conn_id=ELASTICSEARCH_CONN_ID)
    with es.get_conn() as conn:
        try:
            conn.es.indices.create(index_name, body=nfe_index_conf)
        except ElasticsearchRequestError as e:
            if e.error != 'resource_already_exists_exception':
                raise

    file_data[INDEX_KEY] = index_name

    return file_data
