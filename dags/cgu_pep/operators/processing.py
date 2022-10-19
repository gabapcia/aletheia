import csv
from datetime import datetime
import uuid
import re
from collections import deque
from tempfile import NamedTemporaryFile
from zipfile import ZipFile
from typing import Any, Dict, Iterator, List
from elasticsearch.helpers import parallel_bulk
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook
from minio_plugin.hooks.minio_hook import MinioHook
from cgu_pep.operators.file_storage import MINIO_BUCKET, MINIO_CONN_ID
from airflow.decorators import task


ELASTICSEARCH_CONN_ID = 'elasticsearch_default'
ELASTICSEARCH_TIMEOUT = 30
VALID_UFS = [
    'RO',
    'AC',
    'AM',
    'RR',
    'PA',
    'AP',
    'TO',
    'MA',
    'PI',
    'CE',
    'RN',
    'PB',
    'PE',
    'AL',
    'SE',
    'BA',
    'MG',
    'ES',
    'RJ',
    'SP',
    'PR',
    'SC',
    'RS',
    'MS',
    'MT',
    'GO',
    'DF',
]


def _open_zip(filepath: str, header: List[str]) -> Iterator[Dict[str, str]]:
    minio = MinioHook(conn_id=MINIO_CONN_ID)

    with minio.get_object(bucket=MINIO_BUCKET, name=filepath) as f, NamedTemporaryFile(mode='wb') as tmp:
        while data := f.read(8 * 1024):
            tmp.write(data)

        tmp.flush()

        files = []
        with ZipFile(tmp.name, mode='r') as zip:
            if len(zip.namelist()) > 1:
                raise ValueError(f'Expected only one file, but extracted {len(files)}')

            filename = zip.namelist()[0]
            with zip.open(filename, mode='r') as file:
                file.readline()  # drop header

                reader = csv.DictReader(
                    (line.decode('ISO-8859-1') for line in file),
                    delimiter=';',
                    quotechar='"',
                    fieldnames=header,
                    strict=True,
                )

                yield from reader


def _load_people(filepath: str) -> Iterator[Dict[str, Any]]:
    # Source: https://www.portaltransparencia.gov.br/pagina-interna/605518-dicionario-de-dados-pep
    header = [
        'cpf',
        'nome',
        'sigle_funcao',
        'descricao_funcao',
        'nivel_funcao',
        'nome_orgao',
        'data_inicio_exercicio',
        'data_fim_exercicio',
        'data_fim_carencia',
    ]

    reader = _open_zip(filepath=filepath, header=header)
    for line in reader:
        oid = str(uuid.uuid5(uuid.NAMESPACE_OID, ':'.join([
            line['cpf'].upper(),
            line['nome'].upper(),
            line['sigle_funcao'].upper(),
            line['nivel_funcao'].upper(),
            line['nome_orgao'].upper(),
            line['data_inicio_exercicio'].upper(),
        ])))

        if uf := re.search(r'.+-(?P<uf>[A-Z]{2})$', line['nome_orgao'].upper()):
            uf = uf.group('uf')

            if uf not in VALID_UFS:
                uf = None

        person = {
            '_id': oid,
            'cpf': re.sub(r'[^\d\*]', '', line['cpf']),
            'nome': line['nome'].upper(),
            'sigle_funcao': line['sigle_funcao'].upper(),
            'descricao_funcao': line['descricao_funcao'].upper(),
            'nivel_funcao': line['nivel_funcao'].upper(),
            'nome_orgao': line['nome_orgao'].upper(),
            'uf': uf,
            'data_inicio_exercicio': (
                round(datetime.strptime(line['data_inicio_exercicio'], '%d/%m/%Y').timestamp() * 1000)
                if re.match(r'^\d{2}/\d{2}/\d{4}$', line['data_inicio_exercicio'])
                else None
            ),
            'data_fim_exercicio': (
                round(datetime.strptime(line['data_fim_exercicio'], '%d/%m/%Y').timestamp() * 1000)
                if re.match(r'^\d{2}/\d{2}/\d{4}$', line['data_fim_exercicio'])
                else None
            ),
            'data_fim_carencia': (
                round(datetime.strptime(line['data_fim_carencia'], '%d/%m/%Y').timestamp() * 1000)
                if re.match(r'^\d{2}/\d{2}/\d{4}$', line['data_fim_carencia'])
                else None
            ),
        }

        for field, value in person.copy().items():
            if not value:
                person.pop(field)

        yield person


@task(multiple_outputs=False)
def memory(filepath: str, es_indice: str) -> None:
    es = ElasticsearchHook(elasticsearch_conn_id=ELASTICSEARCH_CONN_ID)

    with es.get_conn() as conn:
        people = _load_people(filepath=filepath)

        deque(parallel_bulk(
            client=conn.es,
            actions=(
                {
                    '_index': es_indice,
                    '_id': person.pop('_id'),
                    '_source': person,
                }
                for person in people
            ),
            request_timeout=ELASTICSEARCH_TIMEOUT,
        ), maxlen=0)
