from typing import Dict
from elasticsearch.exceptions import RequestError as ElasticsearchRequestError
from airflow.decorators import task
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook
from tse_candidatos.operators.file_storage import REFERENCE_YEAR_KEY


ELASTICSEARCH_CONN_ID = 'elasticsearch_default'
INDEX_KEY = 'index'


@task(multiple_outputs=False)
def elasticsearch(file_data: Dict[str, str]) -> Dict[str, str]:
    index_name = f'tse-candidatos-{file_data[REFERENCE_YEAR_KEY]}'
    index_conf = {
        'settings': {
            'index.mapping.coerce': False,
            'number_of_shards': 1,
            'number_of_replicas': 0,
        },
        'mappings': {
            'properties': {
                'generation_date': {'type': 'date', 'index': False},
                'election_year': {'type': 'integer'},
                'election_type_code': {'type': 'integer'},
                'election_type': {'type': 'text', 'index': False},
                'election_code': {'type': 'integer'},
                'election_description': {'type': 'text', 'index': False},
                'federative_unit_initials': {'type': 'keyword'},
                'electoral_unit_initials': {'type': 'keyword'},
                'electoral_unit': {'type': 'text', 'index': False},
                'electoral_shift_number': {'type': 'keyword'},
                'election_date': {'type': 'date', 'index': False},
                'election_scope': {'type': 'keyword'},
                'electoral_office_code': {'type': 'integer'},
                'electoral_office': {'type': 'text', 'index': False},
                'candidate_sequential_number': {'type': 'long', 'index': False},
                'number': {'type': 'long'},
                'name': {'type': 'text'},
                'voting_machine_name': {'type': 'text'},
                'social_name': {'type': 'text'},
                'cpf': {'type': 'keyword'},
                'email': {'type': 'keyword'},
                'candidacy_situation_code': {'type': 'integer'},
                'candidacy_situation': {'type': 'text', 'index': False},
                'situation_detail_code': {'type': 'integer'},
                'situation_detail': {'type': 'text', 'index': False},
                'candidacy_organization_type': {'type': 'keyword'},
                'party_number': {'type': 'integer'},
                'party_initial': {'type': 'keyword'},
                'party': {'type': 'text'},
                'coalition_sequential_number': {'type': 'long'},
                'coalition': {'type': 'text', 'index': False},
                'coalition_composition': {'type': 'text', 'index': False},
                'nationality_code': {'type': 'integer'},
                'nationality': {'type': 'keyword'},
                'birth_federative_unit_initials': {'type': 'keyword'},
                'birth_federative_unit_code': {'type': 'integer'},
                'birth_federative_unit': {'type': 'text', 'index': False},
                'birthday': {'type': 'date'},
                'age_at_possession_date': {'type': 'long'},
                'voter_registration_number': {'type': 'long'},
                'gender_code': {'type': 'integer'},
                'gender': {'type': 'keyword', 'index': False},
                'education_degree_code': {'type': 'integer'},
                'education_degree': {'type': 'text', 'index': False},
                'marital_status_code': {'type': 'integer'},
                'marital_status': {'type': 'text', 'index': False},
                'ethnicity_code': {'type': 'integer'},
                'ethnicity': {'type': 'text', 'index': False},
                'occupation_code': {'type': 'integer'},
                'occupation': {'type': 'text', 'index': False},
                'max_amount_expenses': {'type': 'text'},
                'totalization_situation_code': {'type': 'integer'},
                'totalization_situation': {'type': 'text', 'index': False},
                'is_reelection': {'type': 'boolean'},
                'has_assets': {'type': 'boolean'},
                'candidacy_protocol_number': {'type': 'long'},
                'process_number': {'type': 'long'},
                'status_on_election_date_code': {'type': 'integer'},
                'status_on_election_date': {'type': 'text', 'index': False},
                'status_on_voting_machine_code': {'type': 'integer'},
                'status_on_voting_machine': {'type': 'text', 'index': False},
                'available_in_voting_machine': {'type': 'boolean'},
                'federation_initials': {'type': 'keyword'},
                'federation_number': {'type': 'long'},
                'federation': {'type': 'text', 'index': False},
                'status_in_totalization_database_code': {'type': 'integer'},
                'status_in_totalization_database': {'type': 'text', 'index': False},
                'votes_destination_type': {'type': 'text', 'index': False},
                'parties_in_federation': {'type': 'text', 'index': False},
                'is_accountability_situation_ok': {'type': 'boolean'},
                'cancellation_reason': {'type': 'text', 'index': False},
                'assets': {
                    'type': 'nested',
                    'properties': {
                        'order_number': {'type': 'integer', 'index': False},
                        'asset_type_code': {'type': 'integer'},
                        'asset_type': {'type': 'text', 'index': False},
                        'asset': {'type': 'text', 'index': False},
                        'value': {'type': 'long'},
                        'last_update_date': {'type': 'date', 'index': False},
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

    file_data[INDEX_KEY] = index_name

    return file_data
