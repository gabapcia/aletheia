from airflow.decorators import task
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook


@task
def elasticsearch() -> str:
    index_name = 'cgu-pep'
    company_index_conf = {
        'settings': {
            'index.mapping.coerce': False,
            'number_of_shards': 1,
            'number_of_replicas': 0,
        },
        'mappings': {
            'properties': {
                'tax_id': {'type': 'keyword'},
                'name': {'type': 'text'},
                'federal_agency': {'type': 'keyword'},
                'entry_date': {'type': 'date'},
                'exit_date': {'type': 'date'},
                'grace_period_end_date': {'type': 'date'},
                'role_initials': {'type': 'keyword'},
                'role_description': {'type': 'text'},
                'role_level': {'type': 'keyword'},
            }
        }
    }

    es = ElasticsearchHook(elasticsearch_conn_id='elasticsearch_default')
    with es.get_conn() as conn:
        if not conn.es.indices.exists(index=index_name):
            conn.es.indices.create(index_name, body=company_index_conf)

    return index_name
