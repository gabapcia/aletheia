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
        },
        'mappings': {
            'properties': {
                'access_key': {'type': 'keyword', 'index': False},
                'model': {'type': 'keyword', 'index': False},
                'series': {'type': 'keyword', 'index': False},
                'number': {'type': 'keyword', 'index': False},
                'operation_nature': {'type': 'keyword', 'index': False},
                'issue_date': {'type': 'date', 'index': False},
                'most_recent_event': {'type': 'text', 'index': False},
                'most_recent_event_date': {'type': 'date', 'index': False},
                'issuer_tax_id': {'type': 'keyword'},
                'issuer_corporate_name': {
                    'type': 'text',
                    'fields': {
                        'keyword': {'type': 'keyword'},
                    },
                },
                'issuer_state_registration': {'type': 'text', 'index': False},
                'issuer_federative_unit': {'type': 'keyword', 'index': False},
                'issuer_county': {'type': 'text', 'index': False},
                'recipient_cnpj': {'type': 'keyword'},
                'recipient_name': {
                    'type': 'text',
                    'fields': {
                        'keyword': {'type': 'keyword'},
                    },
                },
                'recipient_federative_unit': {'type': 'keyword', 'index': False},
                'recipient_state_tax': {'type': 'text', 'index': False},
                'operation_destination': {'type': 'text', 'index': False},
                'final_costumer': {'type': 'text', 'index': False},
                'buyer_presence': {'type': 'text', 'index': False},
                'value': {'type': 'long', 'index': False},
                'events': {
                    'type': 'nested',
                    'properties': {
                        'event': {'type': 'text', 'index': False},
                        'date': {'type': 'date', 'index': False},
                        'description': {'type': 'text', 'index': False},
                        'reason': {'type': 'text', 'index': False},
                    },
                },
                'items': {
                    'type': 'nested',
                    'properties': {
                        'issuer_tax_id': {'type': 'keyword'},
                        'issuer_corporate_name': {
                            'type': 'text',
                            'fields': {
                                'keyword': {'type': 'keyword'},
                            },
                        },
                        'issuer_state_registration': {'type': 'text', 'index': False},
                        'issuer_federative_unit': {'type': 'keyword', 'index': False},
                        'issuer_county': {'type': 'text', 'index': False},
                        'recipient_cnpj': {'type': 'keyword'},
                        'recipient_name': {
                            'type': 'text',
                            'fields': {
                                'keyword': {'type': 'keyword'},
                            },
                        },
                        'recipient_federative_unit': {'type': 'text', 'index': False},
                        'recipient_state_tax': {'type': 'text', 'index': False},
                        'operation_destination': {'type': 'text', 'index': False},
                        'final_costumer': {'type': 'text', 'index': False},
                        'buyer_presence': {'type': 'text', 'index': False},
                        'product_number': {'type': 'text', 'index': False},
                        'product_description': {'type': 'text', 'index': False},
                        'ncm_sh_code': {'type': 'text', 'index': False},
                        'ncm_sh': {'type': 'text', 'index': False},
                        'cfop': {'type': 'text', 'index': False},
                        'quantity': {'type': 'double', 'index': False},
                        'unit': {'type': 'text', 'index': False},
                        'unit_value': {'type': 'text', 'index': False},
                        'total_value': {'type': 'text', 'index': False},
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
