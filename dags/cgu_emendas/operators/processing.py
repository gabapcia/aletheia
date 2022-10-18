import csv
import uuid
import re
from collections import deque
from tempfile import NamedTemporaryFile
from zipfile import ZipFile
from typing import Any, Dict, Iterator, List
from elasticsearch.helpers import parallel_bulk
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook
from minio_plugin.hooks.minio_hook import MinioHook
from cgu_emendas.operators.file_storage import MINIO_BUCKET, MINIO_CONN_ID
from airflow.decorators import task


ELASTICSEARCH_CONN_ID = 'elasticsearch_default'
ELASTICSEARCH_TIMEOUT = 30


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


def _load_emendas(filepath: str) -> Iterator[Dict[str, Any]]:
    # Source: https://www.portaltransparencia.gov.br/pagina-interna/603482-dicionario-de-dados-emendas-parlamentares
    header = [
        'codigo',
        'ano',
        'codigo_autor',
        'autor',
        'numero',
        'codigo_ibge_municipio',
        'municipio',
        'codigo_ibge_estado',
        'estado',
        'codigo_regiao',
        'regiao',
        'codigo_funcao',
        'funcao',
        'codigo_subfuncao',
        'subfuncao',
        'valor_empenhado',
        'valor_liquidado',
        'valor_pago',
        'restos_a_pagar_inscritos',
        'restos_a_pagar_cancelados',
        'restos_a_pagar_pagos',
    ]

    reader = _open_zip(filepath=filepath, header=header)
    for line in reader:
        oid = str(uuid.uuid5(uuid.NAMESPACE_OID, ':'.join([
            line['codigo'],
            line['ano'],
            line['codigo_autor'],
            line['numero'],
            line['codigo_ibge_municipio'],
            line['codigo_ibge_estado'],
            line['codigo_regiao'],
            line['codigo_funcao'],
            line['codigo_subfuncao'],
        ])))

        emenda = {
            '_id': oid,
            'codigo': line['codigo'],
            'ano': line['ano'],
            'codigo_autor': line['codigo_autor'],
            'autor': line['autor'].upper(),
            'numero': line['numero'],
            'codigo_ibge_municipio': line['codigo_ibge_municipio'],
            'municipio': line['municipio'].upper(),
            'codigo_ibge_estado': line['codigo_ibge_estado'],
            'estado': line['estado'].upper(),
            'codigo_regiao': line['codigo_regiao'],
            'regiao': line['regiao'].upper(),
            'codigo_funcao': line['codigo_funcao'],
            'funcao': line['funcao'].upper(),
            'codigo_subfuncao': line['codigo_subfuncao'],
            'subfuncao': line['subfuncao'].upper(),
            'valor_empenhado': round(float(re.sub(r',', '.', re.sub(r'\.', '', line['valor_empenhado']))) * 100),
            'valor_liquidado': round(float(re.sub(r',', '.', re.sub(r'\.', '', line['valor_liquidado']))) * 100),
            'valor_pago': round(float(re.sub(r',', '.', re.sub(r'\.', '', line['valor_pago']))) * 100),
            'restos_a_pagar_inscritos': round(float(re.sub(r',', '.', re.sub(r'\.', '', line['restos_a_pagar_inscritos']))) * 100),
            'restos_a_pagar_cancelados': round(float(re.sub(r',', '.', re.sub(r'\.', '', line['restos_a_pagar_cancelados']))) * 100),
            'restos_a_pagar_pagos': round(float(re.sub(r',', '.', re.sub(r'\.', '', line['restos_a_pagar_pagos']))) * 100),
        }

        yield emenda


@task(multiple_outputs=False)
def memory(filepath: str, es_indice: str) -> None:
    es = ElasticsearchHook(elasticsearch_conn_id=ELASTICSEARCH_CONN_ID)

    with es.get_conn() as conn:
        emendas = _load_emendas(filepath=filepath)

        deque(parallel_bulk(
            client=conn.es,
            actions=(
                {
                    '_index': es_indice,
                    '_id': emenda.pop('_id'),
                    '_source': emenda,
                }
                for emenda in emendas
            ),
            request_timeout=ELASTICSEARCH_TIMEOUT,
        ), maxlen=0)
