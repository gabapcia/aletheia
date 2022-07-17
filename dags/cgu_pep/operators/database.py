from typing import Any, Dict, Tuple
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook


PEOPLE_INDEX_KEY = 'people'


@task
def people_index() -> str:
    index_name = 'cgu-pep-people'
    company_index_conf = {
        'settings': {
            'index.mapping.coerce': False,
        },
        'mappings': {
            'properties': {
                'tax_id': {'type': 'keyword'},
                'name': {'type': 'text'},
                'role.initials': {'type': 'keyword', 'index': False},
                'role.description': {'type': 'text', 'index': False},
                'role.level': {'type': 'keyword', 'index': False},
                'federal_agency': {'type': 'keyword', 'index': False},
                'entry_date': {'type': 'date', 'index': False},
                'exit_date': {'type': 'date', 'index': False},
                'grace_period_end_date': {'type': 'date', 'index': False}
            }
        }
    }

    es = ElasticsearchHook(elasticsearch_conn_id='elasticsearch_default')
    with es.get_conn() as conn:
        if not conn.es.indices.exists(index=index_name):
            conn.es.indices.create(index_name, body=company_index_conf)

    return index_name


def elasticsearch() -> Tuple[TaskGroup, Dict[str, Any]]:
    with TaskGroup(group_id='elasticsearch') as elasticsearch:
        people_index_name = people_index()

    return elasticsearch, {
        PEOPLE_INDEX_KEY: people_index_name,
    }
