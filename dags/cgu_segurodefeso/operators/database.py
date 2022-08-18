from typing import Dict
from elasticsearch.exceptions import RequestError as ElasticsearchRequestError
from airflow.decorators import task
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook
from cgu_segurodefeso.operators.file_storage import FILEDATE_KEY


ELASTICSEARCH_CONN_ID = 'elasticsearch_default'
INDEX_KEY = 'index'


@task(multiple_outputs=False)
def elasticsearch(file_data: Dict[str, str]) -> Dict[str, str]:
    index_name = f'cgu-segurodefeso-{file_data[FILEDATE_KEY]}'
    people_index_conf = {
        'settings': {
            'index.mapping.coerce': False,
            'number_of_shards': 1,
            'number_of_replicas': 0,
        },
        'mappings': {
            'properties': {
                'year_month_reference': {'type': 'keyword'},
                'federative_unit': {'type': 'keyword'},
                'siafi_county_code': {'type': 'keyword'},
                'county': {'type': 'text', 'index': False},
                'cpf': {'type': 'keyword'},
                'nis': {'type': 'keyword'},
                'rgp': {'type': 'keyword'},
                'name': {
                    'type': 'text',
                    'fields': {
                        'keyword': {'type': 'keyword'},
                    },
                },
                'installment_value': {'type': 'long'},
            },
        },
    }

    es = ElasticsearchHook(elasticsearch_conn_id=ELASTICSEARCH_CONN_ID)
    with es.get_conn() as conn:
        try:
            conn.es.indices.create(index_name, body=people_index_conf)
        except ElasticsearchRequestError as e:
            if e.error != 'resource_already_exists_exception':
                raise

    file_data[INDEX_KEY] = index_name

    return file_data
