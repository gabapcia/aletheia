from airflow.decorators import task
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook


@task(multiple_outputs=False)
def elasticsearch() -> str:
    index_name = 'cgu-emendas'
    emenda_index_conf = {
        'settings': {
            'index.mapping.coerce': False,
            'number_of_shards': 1,
            'number_of_replicas': 0,
        },
        'mappings': {
            'properties': {
                'codigo': {'type': 'keyword'},
                'ano': {'type': 'keyword'},
                'codigo_autor': {'type': 'keyword'},
                'autor': {'type': 'text'},
                'numero': {'type': 'keyword'},
                'codigo_ibge_municipio': {'type': 'keyword'},
                'municipio': {'type': 'text'},
                'codigo_ibge_estado': {'type': 'keyword'},
                'estado': {'type': 'text'},
                'codigo_regiao': {'type': 'keyword'},
                'regiao': {'type': 'text'},
                'codigo_funcao': {'type': 'keyword'},
                'funcao': {'type': 'text'},
                'codigo_subfuncao': {'type': 'keyword'},
                'subfuncao': {'type': 'text'},
                'valor_empenhado': {'type': 'long'},
                'valor_liquidado': {'type': 'long'},
                'valor_pago': {'type': 'long'},
                'restos_a_pagar_inscritos': {'type': 'long'},
                'restos_a_pagar_cancelados': {'type': 'long'},
                'restos_a_pagar_pagos': {'type': 'long'},
            }
        }
    }

    es = ElasticsearchHook(elasticsearch_conn_id='elasticsearch_default')
    with es.get_conn() as conn:
        if not conn.es.indices.exists(index=index_name):
            conn.es.indices.create(index_name, body=emenda_index_conf)

    return index_name
