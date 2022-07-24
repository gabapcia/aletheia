from typing import Dict
from airflow.decorators import task
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook
from cgu_bolsafamilia.operators.file_storage import FILEDATE_KEY


PEOPLE_INDEX_KEY = 'people'


@task(multiple_outputs=False)
def elasticsearch(files: Dict[str, str]) -> Dict[str, str]:
    data = files.copy()

    index_name = f'cgu-bolsafamilia-people-{data[FILEDATE_KEY]}'
    company_index_conf = {
        'settings': {
            'index.mapping.coerce': False,
        },
        'mappings': {
            'properties': {
                'reference_date': {'type': 'date', 'index': False},
                'competency_date': {'type': 'date', 'index': False},
                'federative_unit': {'type': 'keyword', 'index': False},
                'county_siafi_code': {'type': 'keyword', 'index': False},
                'county_siafi': {'type': 'text', 'index': False},
                'tax_id': {'type': 'keyword'},
                'nis': {'type': 'keyword'},
                'name': {
                    'type': 'text',
                    'fields': {
                        'keyword': {'type': 'keyword'},
                    },
                },
                'value': {'type': 'long', 'index': False},
                'withdraws': {
                    'type': 'nested',
                    'properties': {
                        'date': {'type': 'date', 'index': False},
                        'value': {'type': 'long', 'index': False},
                    },
                },
            },
        },
    }

    es = ElasticsearchHook(elasticsearch_conn_id='elasticsearch_default')
    with es.get_conn() as conn:
        if not conn.es.indices.exists(index=index_name):
            conn.es.indices.create(index_name, body=company_index_conf)

    data[PEOPLE_INDEX_KEY] = index_name

    return data
