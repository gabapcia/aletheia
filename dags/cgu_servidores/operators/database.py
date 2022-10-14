from typing import Union, Dict, List
from elasticsearch.exceptions import RequestError as ElasticsearchRequestError
from airflow.decorators import task
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook
from cgu_servidores.operators.scraper import FILEDATE_KEY, RETIRED_KEY, PENSIONER_KEY, EMPLOYEE_KEY
from cgu_servidores.operators.file_storage import FILE_TYPE_KEY


INDEX_KEY = 'index'


@task(multiple_outputs=False)
def choose_catalog_by_type(catalog: List[Dict[str, Union[str, Dict[str, str]]]], filetype: str) -> List[Dict[str, Union[str, Dict[str, str]]]]:
    filtered_catalog = list()

    for item in catalog:
        if item[FILE_TYPE_KEY] == filetype:
            filtered_catalog.append(item)

    return filtered_catalog


@task(multiple_outputs=False)
def employee_index(catalog: Dict[str, Union[str, Dict[str, str]]]) -> Dict[str, Union[str, Dict[str, str]]]:
    index_name = f'cgu-servidores-employee-{catalog[FILEDATE_KEY]}'
    employee_index_conf = {
        'settings': {
            'index.mapping.coerce': False,
        },
        'mappings': {
            'properties': {
                'id_portal': {'type': 'keyword'},
                'nome': {
                    'type': 'text',
                    'fields': {
                        'keyword': {'type': 'keyword'},
                    },
                },
                'cpf': {'type': 'keyword'},
                'matricula': {'type': 'keyword'},
                'cargos': {
                    'type': 'nested',
                    'properties': {
                        'descricao_cargo': {'type': 'text'},
                        'classe_cargo': {'type': 'text'},
                        'referencia_cargo': {'type': 'text'},
                        'padrao_cargo': {'type': 'text'},
                        'nivel_cargo': {'type': 'text'},
                        'sigla_funcao': {'type': 'keyword'},
                        'nivel_funcao': {'type': 'text'},
                        'funcao': {'type': 'text'},
                        'codigo_atividade': {'type': 'keyword'},
                        'atividade': {'type': 'text'},
                        'opcao_parcial': {'type': 'text'},
                        'codigo_unidade_orgao_lotacao': {'type': 'keyword'},
                        'unidade_orgao_lotacao': {'type': 'text', 'index': False},
                        'codigo_orgao_lotacao': {'type': 'keyword'},
                        'orgao_lotacao': {'type': 'text', 'index': False},
                        'codigo_orgao_superior_lotacao': {'type': 'keyword'},
                        'orgao_superior_lotacao': {'type': 'text', 'index': False},
                        'codigo_unidade_orgao_exercicio': {'type': 'keyword'},
                        'unidade_orgao_exercicio': {'type': 'text', 'index': False},
                        'codigo_orgao_exercicio': {'type': 'keyword'},
                        'orgao_exercicio': {'type': 'text', 'index': False},
                        'codigo_orgao_superior_exercicio': {'type': 'keyword'},
                        'orgao_superior_exercicio': {'type': 'text', 'index': False},
                        'codigo_tipo_vinculo': {'type': 'keyword'},
                        'tipo_vinculo': {'type': 'text', 'index': False},
                        'situacao_vinculo': {'type': 'text'},
                        'data_inicio_afastamento': {'type': 'date'},
                        'data_termino_afastamento': {'type': 'date'},
                        'regime_juridico': {'type': 'keyword'},
                        'jornada_trabalho': {'type': 'keyword'},
                        'data_ingresso_cargo_funcao': {'type': 'date'},
                        'data_nomeacao_cargo_funcao': {'type': 'date'},
                        'data_ingresso_orgao': {'type': 'date'},
                        'documento_ingresso_servico_publico': {'type': 'text'},
                        'data_diploma_ingresso': {'type': 'date'},
                        'diploma_ingresso_cargo_funcao': {'type': 'text'},
                        'diploma_ingresso_orgao': {'type': 'text'},
                        'diploma_ingresso_servico_publico': {'type': 'text'},
                        'uf_exercicio': {'type': 'keyword'},
                    },
                },
                'remuneracoes': {
                    'type': 'nested',
                    'properties': {
                        'ano_mes': {'type': 'keyword'},
                        'observacao': {'type': 'text'},
                        'remuneracao_basica_bruca': {'type': 'long'},
                        'remuneracao_basica_bruca_dolar': {'type': 'long'},
                        'abate_teto': {'type': 'long'},
                        'abate_teto_dolar': {'type': 'long'},
                        'gratificacao_natalina': {'type': 'long'},
                        'gratificacao_natalina_dolar': {'type': 'long'},
                        'abate_teto_gratificacao_natalina': {'type': 'long'},
                        'abate_teto_gratificacao_natalina_dolar': {'type': 'long'},
                        'ferias': {'type': 'long'},
                        'ferias_dolar': {'type': 'long'},
                        'outras_reuneracoes_eventuais': {'type': 'long'},
                        'outras_reuneracoes_eventuais_dolar': {'type': 'long'},
                        'irrf': {'type': 'long'},
                        'irrf_dolar': {'type': 'long'},
                        'pss_rpgs': {'type': 'long'},
                        'pss_rpgs_dolar': {'type': 'long'},
                        'demais_deducoes': {'type': 'long'},
                        'demais_deducoes_dolar': {'type': 'long'},
                        'pensao_militar': {'type': 'long'},
                        'pensao_militar_dolar': {'type': 'long'},
                        'fundo_saude': {'type': 'long'},
                        'fundo_saude_dolar': {'type': 'long'},
                        'taxa_ocupacao_imovel_funcional': {'type': 'long'},
                        'taxa_ocupacao_imovel_funcional_dolar': {'type': 'long'},
                        'remuneracao_apos_deducoes_obrigatorias': {'type': 'long'},
                        'remuneracao_apos_deducoes_obrigatorias_dolar': {'type': 'long'},
                        'verbas_indenizatorias': {'type': 'long'},
                        'verbas_indenizatorias_dolar': {'type': 'long'},
                        'verbas_indenizatorias_militar': {'type': 'long'},
                        'verbas_indenizatorias_militar_dolar': {'type': 'long'},
                        'verbas_indenizatorias_desligamento_voluntario': {'type': 'long'},
                        'verbas_indenizatorias_desligamento_voluntario_dolar': {'type': 'long'},
                        'verbas_indenizatorias_total': {'type': 'long'},
                        'verbas_indenizatorias_total_dolar': {'type': 'long'},
                    },
                },
                'afastamentos': {
                    'type': 'nested',
                    'properties': {
                        'ano_mes': {'type': 'keyword'},
                        'data_inicio_afastamento': {'type': 'date'},
                        'data_termino_afastamento': {'type': 'date'},
                    },
                },
                'honorarios': {
                    'properties': {
                        'jetons': {
                            'type': 'nested',
                            'properties': {
                                'ano_mes': {'type': 'keyword'},
                                'empresa': {'type': 'text'},
                                'valor': {'type': 'long'},
                            },
                        },
                        'advocaticios': {
                            'type': 'nested',
                            'properties': {
                                'ano_mes': {'type': 'keyword'},
                                'observacao': {'type': 'text'},
                                'valor': {'type': 'long'},
                            },
                        },
                    },
                },
            },
        },
    }

    es = ElasticsearchHook(elasticsearch_conn_id='elasticsearch_default')
    with es.get_conn() as conn:
        try:
            conn.es.indices.create(index_name, body=employee_index_conf)
        except ElasticsearchRequestError as e:
            if e.error != 'resource_already_exists_exception':
                raise

    catalog[INDEX_KEY] = index_name

    return catalog


@task(multiple_outputs=False)
def retired_index(catalog: Dict[str, Union[str, Dict[str, str]]]) -> Dict[str, Union[str, Dict[str, str]]]:
    index_name = f'cgu-servidores-retired-{catalog[FILEDATE_KEY]}'
    retired_index_conf = {
        'settings': {
            'index.mapping.coerce': False,
        },
        'mappings': {
            'properties': {
                'id_portal': {'type': 'keyword'},
                'nome': {
                    'type': 'text',
                    'fields': {
                        'keyword': {'type': 'keyword'},
                    },
                },
                'cpf': {'type': 'keyword'},
                'matricula': {'type': 'keyword'},
                'cargo': {
                    'properties': {
                        'codigo_tipo_aposentadoria': {'type': 'keyword'},
                        'tipo_aposentadoria': {'type': 'text', 'index': False},
                        'data_aposentadoria': {'type': 'date'},
                        'descricao_cargo': {'type': 'text'},
                        'codigo_unidade_orgao_lotacao': {'type': 'keyword'},
                        'unidade_orgao_lotacao': {'type': 'text', 'index': False},
                        'codigo_orgao_lotacao': {'type': 'keyword'},
                        'orgao_lotacao': {'type': 'text', 'index': False},
                        'codigo_orgao_superior_lotacao': {'type': 'keyword'},
                        'orgao_superior_lotacao': {'type': 'text', 'index': False},
                        'codigo_tipo_vinculo': {'type': 'keyword'},
                        'tipo_vinculo': {'type': 'keyword'},
                        'situacao_vinculo': {'type': 'text'},
                        'regime_juridico': {'type': 'keyword'},
                        'jornada_trabalho': {'type': 'keyword'},
                        'data_ingresso_cargo_funcao': {'type': 'date'},
                        'data_nomeacao_cargo_funcao': {'type': 'date'},
                        'data_ingresso_orgao': {'type': 'date'},
                        'documento_ingresso_servico_publico': {'type': 'text'},
                        'data_diploma_ingresso_servico_publico': {'type': 'date'},
                        'diploma_ingresso_cargo_funcao': {'type': 'text'},
                        'diploma_ingresso_orgao': {'type': 'text'},
                        'diploma_ingresso_servico_publico': {'type': 'text'},
                    },
                },
                'remuneracao': {
                    'properties': {
                        'ano_mes': {'type': 'keyword'},
                        'observacao': {'type': 'text'},
                        'remuneracao_basica_bruca': {'type': 'long'},
                        'remuneracao_basica_bruca_dolar': {'type': 'long'},
                        'abate_teto': {'type': 'long'},
                        'abate_teto_dolar': {'type': 'long'},
                        'gratificacao_natalina': {'type': 'long'},
                        'gratificacao_natalina_dolar': {'type': 'long'},
                        'abate_teto_gratificacao_natalina': {'type': 'long'},
                        'abate_teto_gratificacao_natalina_dolar': {'type': 'long'},
                        'ferias': {'type': 'long'},
                        'ferias_dolar': {'type': 'long'},
                        'outras_reuneracoes_eventuais': {'type': 'long'},
                        'outras_reuneracoes_eventuais_dolar': {'type': 'long'},
                        'irrf': {'type': 'long'},
                        'irrf_dolar': {'type': 'long'},
                        'pss_rpgs': {'type': 'long'},
                        'pss_rpgs_dolar': {'type': 'long'},
                        'demais_deducoes': {'type': 'long'},
                        'demais_deducoes_dolar': {'type': 'long'},
                        'pensao_militar': {'type': 'long'},
                        'pensao_militar_dolar': {'type': 'long'},
                        'fundo_saude': {'type': 'long'},
                        'fundo_saude_dolar': {'type': 'long'},
                        'taxa_ocupacao_imovel_funcional': {'type': 'long'},
                        'taxa_ocupacao_imovel_funcional_dolar': {'type': 'long'},
                        'remuneracao_apos_deducoes_obrigatorias': {'type': 'long'},
                        'remuneracao_apos_deducoes_obrigatorias_dolar': {'type': 'long'},
                        'verbas_indenizatorias': {'type': 'long'},
                        'verbas_indenizatorias_dolar': {'type': 'long'},
                        'verbas_indenizatorias_militar': {'type': 'long'},
                        'verbas_indenizatorias_militar_dolar': {'type': 'long'},
                        'verbas_indenizatorias_desligamento_voluntario': {'type': 'long'},
                        'verbas_indenizatorias_desligamento_voluntario_dolar': {'type': 'long'},
                        'verbas_indenizatorias_total': {'type': 'long'},
                        'verbas_indenizatorias_total_dolar': {'type': 'long'},
                    },
                },
                'honorarios': {
                    'properties': {
                        'jetons': {
                            'type': 'nested',
                            'properties': {
                                'ano_mes': {'type': 'keyword'},
                                'empresa': {'type': 'text'},
                                'valor': {'type': 'long'},
                            },
                        },
                        'advocaticios': {
                            'type': 'nested',
                            'properties': {
                                'ano_mes': {'type': 'keyword'},
                                'observacao': {'type': 'text'},
                                'valor': {'type': 'long'},
                            },
                        },
                    },
                },
            },
        },
    }

    es = ElasticsearchHook(elasticsearch_conn_id='elasticsearch_default')
    with es.get_conn() as conn:
        try:
            conn.es.indices.create(index_name, body=retired_index_conf)
        except ElasticsearchRequestError as e:
            if e.error != 'resource_already_exists_exception':
                raise

    catalog[INDEX_KEY] = index_name

    return catalog


@task(multiple_outputs=False)
def pensioner_index(catalog: Dict[str, Union[str, Dict[str, str]]]) -> Dict[str, Union[str, Dict[str, str]]]:
    index_name = f'cgu-servidores-pensioner-{catalog[FILEDATE_KEY]}'
    pensioner_index_conf = {
        'settings': {
            'index.mapping.coerce': False,
        },
        'mappings': {
            'properties': {
                'id_portal': {'type': 'keyword'},
                'nome': {
                    'type': 'text',
                    'fields': {
                        'keyword': {'type': 'keyword'},
                    },
                },
                'cpf': {'type': 'keyword'},
                'matricula': {'type': 'keyword'},
                'instituidores_pensao': {
                    'type': 'nested',
                    'properties': {
                        'cpf_representante_legal': {'type': 'keyword'},
                        'nome_representante_legal': {
                            'type': 'text',
                            'fields': {
                                'keyword': {'type': 'keyword'},
                            },
                        },
                        'cpf_instituidor_pensao': {'type': 'keyword'},
                        'nome_instituidor_pensao': {
                            'type': 'text',
                            'fields': {
                                'keyword': {'type': 'keyword'},
                            },
                        },
                        'codigo_tipo_pensao': {'type': 'keyword'},
                        'tipo_pensao': {'type': 'text', 'index': False},
                        'data_inicio_pensao': {'type': 'date'},
                        'descricao_cargo_instituidor_pensao': {'type': 'text'},
                        'codigo_unidade_orgao_lotacao_instituidor_pensao': {'type': 'keyword'},
                        'unidade_orgao_lotacao_instituidor_pensao': {'type': 'text', 'index': False},
                        'codigo_orgao_lotacao_instituidor_pensao': {'type': 'keyword'},
                        'orgao_lotacao_instituidor_pensao': {'type': 'text', 'index': False},
                        'codigo_orgao_superior_lotacao_instituidor_pensao': {'type': 'keyword'},
                        'orgao_superior_lotacao_instituidor_pensao': {'type': 'text', 'index': False},
                        'codigo_tipo_vinculo': {'type': 'keyword'},
                        'tipo_vinculo': {'type': 'keyword'},
                        'situacao_vinculo': {'type': 'text'},
                        'regime_juridico_instituidor_pensao': {'type': 'keyword'},
                        'jornada_trabalho_instituidor_pensao': {'type': 'keyword'},
                        'data_ingresso_cargo_funcao_instituidor_pensao': {'type': 'date'},
                        'data_nomeacao_cargo_funcao_instituidor_pensao': {'type': 'date'},
                        'data_ingresso_orgao_instituidor_pensao': {'type': 'date'},
                        'documento_ingresso_servico_publico_instituidor_pensao': {'type': 'text'},
                        'data_diploma_ingresso_servico_publico_instituidor_pensao': {'type': 'date'},
                        'diploma_ingresso_cargo_funcao_instituidor_pensao': {'type': 'text'},
                        'diploma_ingresso_orgao_instituidor_pensao': {'type': 'text'},
                        'diploma_ingresso_servico_publico_instituidor_pensao': {'type': 'text'},
                    },
                },
                'remuneracoes': {
                    'type': 'nested',
                    'properties': {
                        'ano_mes': {'type': 'keyword'},
                        'observacao': {'type': 'text'},
                        'remuneracao_basica_bruca': {'type': 'long'},
                        'remuneracao_basica_bruca_dolar': {'type': 'long'},
                        'abate_teto': {'type': 'long'},
                        'abate_teto_dolar': {'type': 'long'},
                        'gratificacao_natalina': {'type': 'long'},
                        'gratificacao_natalina_dolar': {'type': 'long'},
                        'abate_teto_gratificacao_natalina': {'type': 'long'},
                        'abate_teto_gratificacao_natalina_dolar': {'type': 'long'},
                        'ferias': {'type': 'long'},
                        'ferias_dolar': {'type': 'long'},
                        'outras_reuneracoes_eventuais': {'type': 'long'},
                        'outras_reuneracoes_eventuais_dolar': {'type': 'long'},
                        'irrf': {'type': 'long'},
                        'irrf_dolar': {'type': 'long'},
                        'pss_rpgs': {'type': 'long'},
                        'pss_rpgs_dolar': {'type': 'long'},
                        'demais_deducoes': {'type': 'long'},
                        'demais_deducoes_dolar': {'type': 'long'},
                        'pensao_militar': {'type': 'long'},
                        'pensao_militar_dolar': {'type': 'long'},
                        'fundo_saude': {'type': 'long'},
                        'fundo_saude_dolar': {'type': 'long'},
                        'taxa_ocupacao_imovel_funcional': {'type': 'long'},
                        'taxa_ocupacao_imovel_funcional_dolar': {'type': 'long'},
                        'remuneracao_apos_deducoes_obrigatorias': {'type': 'long'},
                        'remuneracao_apos_deducoes_obrigatorias_dolar': {'type': 'long'},
                        'verbas_indenizatorias': {'type': 'long'},
                        'verbas_indenizatorias_dolar': {'type': 'long'},
                        'verbas_indenizatorias_militar': {'type': 'long'},
                        'verbas_indenizatorias_militar_dolar': {'type': 'long'},
                        'verbas_indenizatorias_desligamento_voluntario': {'type': 'long'},
                        'verbas_indenizatorias_desligamento_voluntario_dolar': {'type': 'long'},
                        'verbas_indenizatorias_total': {'type': 'long'},
                        'verbas_indenizatorias_total_dolar': {'type': 'long'},
                    },
                },
                'honorarios': {
                    'properties': {
                        'jetons': {
                            'type': 'nested',
                            'properties': {
                                'ano_mes': {'type': 'keyword'},
                                'empresa': {'type': 'text'},
                                'valor': {'type': 'long'},
                            },
                        },
                        'advocaticios': {
                            'type': 'nested',
                            'properties': {
                                'ano_mes': {'type': 'keyword'},
                                'observacao': {'type': 'text'},
                                'valor': {'type': 'long'},
                            },
                        },
                    },
                },
            },
        },
    }

    es = ElasticsearchHook(elasticsearch_conn_id='elasticsearch_default')
    with es.get_conn() as conn:
        try:
            conn.es.indices.create(index_name, body=pensioner_index_conf)
        except ElasticsearchRequestError as e:
            if e.error != 'resource_already_exists_exception':
                raise

    catalog[INDEX_KEY] = index_name

    return catalog


def elasticsearch(catalog: List[Dict[str, Union[str, Dict[str, str]]]], filetype: str) -> List[Dict[str, Union[str, Dict[str, str]]]]:
    filtered_catalog = choose_catalog_by_type.override(task_id=f'get_{filetype}_files')(catalog=catalog, filetype=filetype)

    return {
        EMPLOYEE_KEY: employee_index,
        RETIRED_KEY: retired_index,
        PENSIONER_KEY: pensioner_index,
    }[filetype].expand(catalog=filtered_catalog)
