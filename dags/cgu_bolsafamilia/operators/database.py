from typing import Dict
from airflow.decorators import task
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook
from cgu_bolsafamilia.operators.file_storage import FILEDATE_KEY


PEOPLE_INDEX_KEY = 'people'


@task(multiple_outputs=False)
def elasticsearch(files: Dict[str, str]) -> Dict[str, str]:
    data = files.copy()

    index_name = f'cgu-bolsafamilia-{data[FILEDATE_KEY]}'
    company_index_conf = {
        'settings': {
            'index.mapping.coerce': False,
            'number_of_shards': 1,
            'number_of_replicas': 0,
        },
        'mappings': {
            'properties': {
                'reference_date': {'type': 'date'},
                'competency_date': {'type': 'date'},
                'federative_unit': {'type': 'keyword'},
                'county_siafi_code': {'type': 'keyword'},
                'county_siafi': {'type': 'keyword'},
                'tax_id': {'type': 'keyword'},
                'nis': {'type': 'keyword'},
                'name': {'type': 'text'},
                'value': {'type': 'long'},
                'withdraws': {
                    'type': 'nested',
                    'properties': {
                        'date': {'type': 'date'},
                        'value': {'type': 'long'},
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
