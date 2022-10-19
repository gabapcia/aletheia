from airflow.decorators import task
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook


@task(multiple_outputs=False)
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
                'cpf': {'type': 'keyword'},
                'nome': {'type': 'text'},
                'sigle_funcao': {'type': 'keyword'},
                'descricao_funcao': {'type': 'keyword'},
                'nivel_funcao': {'type': 'keyword'},
                'nome_orgao': {'type': 'keyword'},
                'uf': {'type': 'keyword'},
                'data_inicio_exercicio': {'type': 'date'},
                'data_fim_exercicio': {'type': 'date'},
                'data_fim_carencia': {'type': 'date'},
            }
        }
    }

    es = ElasticsearchHook(elasticsearch_conn_id='elasticsearch_default')
    with es.get_conn() as conn:
        if not conn.es.indices.exists(index=index_name):
            conn.es.indices.create(index_name, body=company_index_conf)

    return index_name
