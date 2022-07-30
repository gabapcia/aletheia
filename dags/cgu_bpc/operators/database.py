from typing import Dict
from elasticsearch.exceptions import RequestError as ElasticsearchRequestError
from airflow.decorators import task
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook
from cgu_bpc.operators.file_storage import FILEDATE_KEY


ELASTICSEARCH_CONN_ID = 'elasticsearch_default'
INDEX_KEY = 'index'


@task(multiple_outputs=False)
def elasticsearch(file_data: Dict[str, str]) -> Dict[str, str]:
    index_name = f"cgu-bpc-{file_data[FILEDATE_KEY]}"
    people_index_conf = {
        "settings": {
            "index.mapping.coerce": False,
        },
        "mappings": {
            "properties": {
                "year_month_competence": {"type": "keyword", "index": False},
                "year_month_reference": {"type": "keyword", "index": False},
                "federative_unit": {"type": "keyword", "index": False},
                "siafi_county_code": {"type": "keyword", "index": False},
                "county": {"type": "text", "index": False},
                "benefit_number": {"type": "keyword", "index": False},
                "benefit_granted_in_court": {"type": "boolean", "index": False},
                "installment_value": {"type": "long", "index": False},
                "nis": {"type": "keyword"},
                "cpf": {"type": "keyword"},
                "name": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword"},
                    },
                },
                "legal_representative": {
                    "properties": {
                        "nis": {"type": "keyword"},
                        "cpf": {"type": "keyword"},
                        "name": {
                            "type": "text",
                            "fields": {
                                "keyword": {"type": "keyword"},
                            },
                        },
                    },
                },
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
