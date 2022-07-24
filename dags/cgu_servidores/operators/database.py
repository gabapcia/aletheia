from typing import Dict, List
from elasticsearch.exceptions import RequestError as ElasticsearchRequestError
from airflow.decorators import task
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook
from cgu_servidores.operators.scraper import RETIRED_KEY, PENSIONER_KEY, EMPLOYEE_KEY
from cgu_servidores.operators.aggregation import FILEDATE_KEY


INDEX_KEY = 'index'


@task(multiple_outputs=False)
def employee(named_files: Dict[str, str]) -> Dict[str, str]:
    index_name = f'cgu-servidores-employee-{named_files[FILEDATE_KEY]}'
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
                'matricula': {'type': 'keyword', 'index': False},
                'cargos': {
                    'type': 'nested',
                    'properties': {
                        'descricao_cargo': {'type': 'text', 'index': False},
                        'classe_cargo': {'type': 'text', 'index': False},
                        'referencia_cargo': {'type': 'text', 'index': False},
                        'padrao_cargo': {'type': 'text', 'index': False},
                        'nivel_cargo': {'type': 'text', 'index': False},
                        'sigla_funcao': {'type': 'keyword', 'index': False},
                        'nivel_funcao': {'type': 'text', 'index': False},
                        'funcao': {'type': 'text', 'index': False},
                        'codigo_atividade': {'type': 'keyword', 'index': False},
                        'atividade': {'type': 'text', 'index': False},
                        'opcao_parcial': {'type': 'text', 'index': False},
                        'codigo_unidade_orgao_lotacao': {'type': 'keyword', 'index': False},
                        'unidade_orgao_lotacao': {'type': 'text', 'index': False},
                        'codigo_orgao_lotacao': {'type': 'keyword', 'index': False},
                        'orgao_lotacao': {'type': 'text', 'index': False},
                        'codigo_orgao_superior_lotacao': {'type': 'keyword', 'index': False},
                        'orgao_superior_lotacao': {'type': 'text', 'index': False},
                        'codigo_unidade_orgao_exercicio': {'type': 'keyword', 'index': False},
                        'unidade_orgao_exercicio': {'type': 'text', 'index': False},
                        'codigo_orgao_exercicio': {'type': 'keyword', 'index': False},
                        'orgao_exercicio': {'type': 'text', 'index': False},
                        'codigo_orgao_superior_exercicio': {'type': 'keyword', 'index': False},
                        'orgao_superior_exercicio': {'type': 'text', 'index': False},
                        'codigo_tipo_vinculo': {'type': 'keyword', 'index': False},
                        'tipo_vinculo': {'type': 'text', 'index': False},
                        'situacao_vinculo': {'type': 'text', 'index': False},
                        'data_inicio_afastamento': {'type': 'date', 'index': False},
                        'data_termino_afastamento': {'type': 'date', 'index': False},
                        'regime_juridico': {'type': 'text', 'index': False},
                        'jornada_trabalho': {'type': 'text', 'index': False},
                        'data_ingresso_cargo_funcao': {'type': 'date', 'index': False},
                        'data_nomeacao_cargo_funcao': {'type': 'date', 'index': False},
                        'data_ingresso_orgao': {'type': 'date', 'index': False},
                        'documento_ingresso_servico_publico': {'type': 'text', 'index': False},
                        'data_diploma_ingresso': {'type': 'date', 'index': False},
                        'diploma_ingresso_cargo_funcao': {'type': 'text', 'index': False},
                        'diploma_ingresso_orgao': {'type': 'text', 'index': False},
                        'diploma_ingresso_servico_publico': {'type': 'text', 'index': False},
                        'uf_exercicio': {'type': 'keyword', 'index': False},
                    },
                },
                'remuneracoes': {
                    'type': 'nested',
                    'properties': {
                        'ano_mes': {'type': 'keyword', 'index': False},
                        'observacao': {'type': 'text', 'index': False},
                        'remuneracao_basica_bruca': {'type': 'long', 'index': False},
                        'remuneracao_basica_bruca_dolar': {'type': 'long', 'index': False},
                        'abate_teto': {'type': 'long', 'index': False},
                        'abate_teto_dolar': {'type': 'long', 'index': False},
                        'gratificacao_natalina': {'type': 'long', 'index': False},
                        'gratificacao_natalina_dolar': {'type': 'long', 'index': False},
                        'abate_teto_gratificacao_natalina': {'type': 'long', 'index': False},
                        'abate_teto_gratificacao_natalina_dolar': {'type': 'long', 'index': False},
                        'ferias': {'type': 'long', 'index': False},
                        'ferias_dolar': {'type': 'long', 'index': False},
                        'outras_reuneracoes_eventuais': {'type': 'long', 'index': False},
                        'outras_reuneracoes_eventuais_dolar': {'type': 'long', 'index': False},
                        'irrf': {'type': 'long', 'index': False},
                        'irrf_dolar': {'type': 'long', 'index': False},
                        'pss_rpgs': {'type': 'long', 'index': False},
                        'pss_rpgs_dolar': {'type': 'long', 'index': False},
                        'demais_deducoes': {'type': 'long', 'index': False},
                        'demais_deducoes_dolar': {'type': 'long', 'index': False},
                        'pensao_militar': {'type': 'long', 'index': False},
                        'pensao_militar_dolar': {'type': 'long', 'index': False},
                        'fundo_saude': {'type': 'long', 'index': False},
                        'fundo_saude_dolar': {'type': 'long', 'index': False},
                        'taxa_ocupacao_imovel_funcional': {'type': 'long', 'index': False},
                        'taxa_ocupacao_imovel_funcional_dolar': {'type': 'long', 'index': False},
                        'remuneracao_apos_deducoes_obrigatorias': {'type': 'long', 'index': False},
                        'remuneracao_apos_deducoes_obrigatorias_dolar': {'type': 'long', 'index': False},
                        'verbas_indenizatorias': {'type': 'long', 'index': False},
                        'verbas_indenizatorias_dolar': {'type': 'long', 'index': False},
                        'verbas_indenizatorias_militar': {'type': 'long', 'index': False},
                        'verbas_indenizatorias_militar_dolar': {'type': 'long', 'index': False},
                        'verbas_indenizatorias_desligamento_voluntario': {'type': 'long', 'index': False},
                        'verbas_indenizatorias_desligamento_voluntario_dolar': {'type': 'long', 'index': False},
                        'verbas_indenizatorias_total': {'type': 'long', 'index': False},
                        'verbas_indenizatorias_total_dolar': {'type': 'long', 'index': False},
                    },
                },
                'afastamentos': {
                    'type': 'nested',
                    'properties': {
                        'ano_mes': {'type': 'keyword', 'index': False},
                        'data_inicio_afastamento': {'type': 'date', 'index': False},
                        'data_termino_afastamento': {'type': 'date', 'index': False},
                    },
                },
                'honorarios': {
                    'properties': {
                        'jetons': {
                            'type': 'nested',
                            'properties': {
                                'ano_mes': {'type': 'keyword', 'index': False},
                                'empresa': {'type': 'text', 'index': False},
                                'valor': {'type': 'long', 'index': False},
                            },
                        },
                        'advocaticios': {
                            'type': 'nested',
                            'properties': {
                                'ano_mes': {'type': 'keyword', 'index': False},
                                'observacao': {'type': 'text', 'index': False},
                                'valor': {'type': 'long', 'index': False},
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

    named_files[INDEX_KEY] = index_name

    return named_files


@task(multiple_outputs=False)
def retired(named_files: Dict[str, str]) -> Dict[str, str]:
    index_name = f'cgu-servidores-retired-{named_files[FILEDATE_KEY]}'
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
                'matricula': {'type': 'keyword', 'index': False},
                'cargo': {
                    'properties': {
                        'codigo_tipo_aposentadoria': {'type': 'text', 'index': False},
                        'tipo_aposentadoria': {'type': 'text', 'index': False},
                        'data_aposentadoria': {'type': 'date', 'index': False},
                        'descricao_cargo': {'type': 'text', 'index': False},
                        'codigo_unidade_orgao_lotacao': {'type': 'text', 'index': False},
                        'unidade_orgao_lotacao': {'type': 'text', 'index': False},
                        'codigo_orgao_lotacao': {'type': 'text', 'index': False},
                        'orgao_lotacao': {'type': 'text', 'index': False},
                        'codigo_orgao_superior_lotacao': {'type': 'text', 'index': False},
                        'orgao_superior_lotacao': {'type': 'text', 'index': False},
                        'codigo_tipo_vinculo': {'type': 'text', 'index': False},
                        'tipo_vinculo': {'type': 'text', 'index': False},
                        'situacao_vinculo': {'type': 'text', 'index': False},
                        'regime_juridico': {'type': 'text', 'index': False},
                        'jornada_trabalho': {'type': 'text', 'index': False},
                        'data_ingresso_cargo_funcao': {'type': 'date', 'index': False},
                        'data_nomeacao_cargo_funcao': {'type': 'date', 'index': False},
                        'data_ingresso_orgao': {'type': 'date', 'index': False},
                        'documento_ingresso_servico_publico': {'type': 'text', 'index': False},
                        'data_diploma_ingresso_servico_publico': {'type': 'date', 'index': False},
                        'diploma_ingresso_cargo_funcao': {'type': 'text', 'index': False},
                        'diploma_ingresso_orgao': {'type': 'text', 'index': False},
                        'diploma_ingresso_servico_publico': {'type': 'text', 'index': False},
                    },
                },
                'remuneracao': {
                    'properties': {
                        'ano_mes': {'type': 'keyword', 'index': False},
                        'observacao': {'type': 'text', 'index': False},
                        'remuneracao_basica_bruca': {'type': 'long', 'index': False},
                        'remuneracao_basica_bruca_dolar': {'type': 'long', 'index': False},
                        'abate_teto': {'type': 'long', 'index': False},
                        'abate_teto_dolar': {'type': 'long', 'index': False},
                        'gratificacao_natalina': {'type': 'long', 'index': False},
                        'gratificacao_natalina_dolar': {'type': 'long', 'index': False},
                        'abate_teto_gratificacao_natalina': {'type': 'long', 'index': False},
                        'abate_teto_gratificacao_natalina_dolar': {'type': 'long', 'index': False},
                        'ferias': {'type': 'long', 'index': False},
                        'ferias_dolar': {'type': 'long', 'index': False},
                        'outras_reuneracoes_eventuais': {'type': 'long', 'index': False},
                        'outras_reuneracoes_eventuais_dolar': {'type': 'long', 'index': False},
                        'irrf': {'type': 'long', 'index': False},
                        'irrf_dolar': {'type': 'long', 'index': False},
                        'pss_rpgs': {'type': 'long', 'index': False},
                        'pss_rpgs_dolar': {'type': 'long', 'index': False},
                        'demais_deducoes': {'type': 'long', 'index': False},
                        'demais_deducoes_dolar': {'type': 'long', 'index': False},
                        'pensao_militar': {'type': 'long', 'index': False},
                        'pensao_militar_dolar': {'type': 'long', 'index': False},
                        'fundo_saude': {'type': 'long', 'index': False},
                        'fundo_saude_dolar': {'type': 'long', 'index': False},
                        'taxa_ocupacao_imovel_funcional': {'type': 'long', 'index': False},
                        'taxa_ocupacao_imovel_funcional_dolar': {'type': 'long', 'index': False},
                        'remuneracao_apos_deducoes_obrigatorias': {'type': 'long', 'index': False},
                        'remuneracao_apos_deducoes_obrigatorias_dolar': {'type': 'long', 'index': False},
                        'verbas_indenizatorias': {'type': 'long', 'index': False},
                        'verbas_indenizatorias_dolar': {'type': 'long', 'index': False},
                        'verbas_indenizatorias_militar': {'type': 'long', 'index': False},
                        'verbas_indenizatorias_militar_dolar': {'type': 'long', 'index': False},
                        'verbas_indenizatorias_desligamento_voluntario': {'type': 'long', 'index': False},
                        'verbas_indenizatorias_desligamento_voluntario_dolar': {'type': 'long', 'index': False},
                        'verbas_indenizatorias_total': {'type': 'long', 'index': False},
                        'verbas_indenizatorias_total_dolar': {'type': 'long', 'index': False},
                    },
                },
                'honorarios': {
                    'properties': {
                        'jetons': {
                            'type': 'nested',
                            'properties': {
                                'ano_mes': {'type': 'keyword', 'index': False},
                                'empresa': {'type': 'text', 'index': False},
                                'valor': {'type': 'long', 'index': False},
                            },
                        },
                        'advocaticios': {
                            'type': 'nested',
                            'properties': {
                                'ano_mes': {'type': 'keyword', 'index': False},
                                'observacao': {'type': 'text', 'index': False},
                                'valor': {'type': 'long', 'index': False},
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

    named_files[INDEX_KEY] = index_name

    return named_files


@task(multiple_outputs=False)
def pensioner(named_files: Dict[str, str]) -> Dict[str, str]:
    index_name = f'cgu-servidores-pensioner-{named_files[FILEDATE_KEY]}'
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
                'matricula': {'type': 'keyword', 'index': False},
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
                        'codigo_tipo_pensao': {'type': 'text', 'index': False},
                        'tipo_pensao': {'type': 'text', 'index': False},
                        'data_inicio_pensao': {'type': 'date', 'index': False},
                        'descricao_cargo_instituidor_pensao': {'type': 'text', 'index': False},
                        'codigo_unidade_orgao_lotacao_instituidor_pensao': {'type': 'text', 'index': False},
                        'unidade_orgao_lotacao_instituidor_pensao': {'type': 'text', 'index': False},
                        'codigo_orgao_lotacao_instituidor_pensao': {'type': 'text', 'index': False},
                        'orgao_lotacao_instituidor_pensao': {'type': 'text', 'index': False},
                        'codigo_orgao_superior_lotacao_instituidor_pensao': {'type': 'text', 'index': False},
                        'orgao_superior_lotacao_instituidor_pensao': {'type': 'text', 'index': False},
                        'codigo_tipo_vinculo': {'type': 'text', 'index': False},
                        'tipo_vinculo': {'type': 'text', 'index': False},
                        'situacao_vinculo': {'type': 'text', 'index': False},
                        'regime_juridico_instituidor_pensao': {'type': 'text', 'index': False},
                        'jornada_trabalho_instituidor_pensao': {'type': 'text', 'index': False},
                        'data_ingresso_cargo_funcao_instituidor_pensao': {'type': 'date', 'index': False},
                        'data_nomeacao_cargo_funcao_instituidor_pensao': {'type': 'date', 'index': False},
                        'data_ingresso_orgao_instituidor_pensao': {'type': 'date', 'index': False},
                        'documento_ingresso_servico_publico_instituidor_pensao': {'type': 'text', 'index': False},
                        'data_diploma_ingresso_servico_publico_instituidor_pensao': {'type': 'date', 'index': False},
                        'diploma_ingresso_cargo_funcao_instituidor_pensao': {'type': 'text', 'index': False},
                        'diploma_ingresso_orgao_instituidor_pensao': {'type': 'text', 'index': False},
                        'diploma_ingresso_servico_publico_instituidor_pensao': {'type': 'text', 'index': False},
                    },
                },
                'remuneracoes': {
                    'type': 'nested',
                    'properties': {
                        'ano_mes': {'type': 'keyword', 'index': False},
                        'observacao': {'type': 'text', 'index': False},
                        'remuneracao_basica_bruca': {'type': 'long', 'index': False},
                        'remuneracao_basica_bruca_dolar': {'type': 'long', 'index': False},
                        'abate_teto': {'type': 'long', 'index': False},
                        'abate_teto_dolar': {'type': 'long', 'index': False},
                        'gratificacao_natalina': {'type': 'long', 'index': False},
                        'gratificacao_natalina_dolar': {'type': 'long', 'index': False},
                        'abate_teto_gratificacao_natalina': {'type': 'long', 'index': False},
                        'abate_teto_gratificacao_natalina_dolar': {'type': 'long', 'index': False},
                        'ferias': {'type': 'long', 'index': False},
                        'ferias_dolar': {'type': 'long', 'index': False},
                        'outras_reuneracoes_eventuais': {'type': 'long', 'index': False},
                        'outras_reuneracoes_eventuais_dolar': {'type': 'long', 'index': False},
                        'irrf': {'type': 'long', 'index': False},
                        'irrf_dolar': {'type': 'long', 'index': False},
                        'pss_rpgs': {'type': 'long', 'index': False},
                        'pss_rpgs_dolar': {'type': 'long', 'index': False},
                        'demais_deducoes': {'type': 'long', 'index': False},
                        'demais_deducoes_dolar': {'type': 'long', 'index': False},
                        'pensao_militar': {'type': 'long', 'index': False},
                        'pensao_militar_dolar': {'type': 'long', 'index': False},
                        'fundo_saude': {'type': 'long', 'index': False},
                        'fundo_saude_dolar': {'type': 'long', 'index': False},
                        'taxa_ocupacao_imovel_funcional': {'type': 'long', 'index': False},
                        'taxa_ocupacao_imovel_funcional_dolar': {'type': 'long', 'index': False},
                        'remuneracao_apos_deducoes_obrigatorias': {'type': 'long', 'index': False},
                        'remuneracao_apos_deducoes_obrigatorias_dolar': {'type': 'long', 'index': False},
                        'verbas_indenizatorias': {'type': 'long', 'index': False},
                        'verbas_indenizatorias_dolar': {'type': 'long', 'index': False},
                        'verbas_indenizatorias_militar': {'type': 'long', 'index': False},
                        'verbas_indenizatorias_militar_dolar': {'type': 'long', 'index': False},
                        'verbas_indenizatorias_desligamento_voluntario': {'type': 'long', 'index': False},
                        'verbas_indenizatorias_desligamento_voluntario_dolar': {'type': 'long', 'index': False},
                        'verbas_indenizatorias_total': {'type': 'long', 'index': False},
                        'verbas_indenizatorias_total_dolar': {'type': 'long', 'index': False},
                    },
                },
                'honorarios': {
                    'properties': {
                        'jetons': {
                            'type': 'nested',
                            'properties': {
                                'ano_mes': {'type': 'keyword', 'index': False},
                                'empresa': {'type': 'text', 'index': False},
                                'valor': {'type': 'long', 'index': False},
                            },
                        },
                        'advocaticios': {
                            'type': 'nested',
                            'properties': {
                                'ano_mes': {'type': 'keyword', 'index': False},
                                'observacao': {'type': 'text', 'index': False},
                                'valor': {'type': 'long', 'index': False},
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

    named_files[INDEX_KEY] = index_name

    return named_files


def elasticsearch(named_files: List[Dict[str, str]], filetype: str) -> List[Dict[str, str]]:
    return {
        EMPLOYEE_KEY: employee,
        RETIRED_KEY: retired,
        PENSIONER_KEY: pensioner,
    }[filetype].override(task_id=f'create_elasticsearch_{filetype}_index').expand(named_files=named_files)
