from elasticsearch.exceptions import RequestError as ElasticsearchRequestError
from airflow.decorators import task
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook


ELASTICSEARCH_CONN_ID = 'elasticsearch_default'


@task(multiple_outputs=False)
def elasticsearch() -> str:
    index_name = 'rfb-cnpj'
    index_conf = {
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
                'type_code': {'type': 'integer'},
                'type': {'type': 'keyword'},
                'situation_code': {'type': 'integer'},
                'situation': {'type': 'keyword'},
                'date_situation': {'type': 'date'},
                'reason_situation_code': {'type': 'integer'},
                'reason_situation': {'type': 'keyword'},
                'start_date': {'type': 'date'},
                'cnae': {'type': 'keyword'},
                'cnae_description': {'type': 'keyword'},
                'other_cnaes': {'type': 'flattened'},
                'address': {'type': 'text'},
                'number': {'type': 'text'},
                'complement': {'type': 'text'},
                'district': {'type': 'text'},
                'zip_code': {'type': 'keyword'},
                'federative_unit': {'type': 'keyword'},
                'county_code': {'type': 'integer'},
                'county': {'type': 'keyword'},
                'country_code': {'type': 'integer'},
                'country': {'type': 'keyword'},
                'foreign_city_name': {'type': 'keyword'},
                'special_situation': {'type': 'keyword'},
                'date_special_situation': {'type': 'date'},
                'phone_1': {'type': 'keyword'},
                'phone_2': {'type': 'keyword'},
                'fax': {'type': 'keyword'},
                'legal_nature_code': {'type': 'integer'},
                'legal_nature': {'type': 'keyword'},
                'share_capital': {'type': 'long'},
                'size_code': {'type': 'integer'},
                'size': {'type': 'keyword'},
                'responsible_qualification_code': {'type': 'integer'},
                'responsible_qualification': {'type': 'keyword'},
                'responsible_federative_entity': {'type': 'keyword'},
                'opted_for_simples': {'type': 'keyword'},
                'date_opted_for_simples': {'type': 'date'},
                'simples_exclusion_date': {'type': 'date'},
                'opted_for_mei': {'type': 'keyword'},
                'date_opted_for_mei': {'type': 'date'},
                'mei_exclusion_date': {'type': 'date'},
                'partners': {
                    'type': 'nested',
                    'properties': {
                        'type_code': {'type': 'integer'},
                        'type': {'type': 'keyword'},
                        'name': {'type': 'text'},
                        'tax_id': {'type': 'keyword'},
                        'qualification_code': {'type': 'integer'},
                        'qualification': {'type': 'keyword'},
                        'join_date': {'type': 'date'},
                        'age_group_code': {'type': 'integer'},
                        'age_group': {'type': 'keyword'},
                        'legal_representative': {
                            'properties': {
                                'name': {'type': 'text'},
                                'tax_id': {'type': 'keyword'},
                                'qualification_code': {'type': 'integer'},
                                'qualification': {'type': 'keyword'},
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
            conn.es.indices.create(index_name, body=index_conf)
        except ElasticsearchRequestError as e:
            if e.error != 'resource_already_exists_exception':
                raise

    return index_name
