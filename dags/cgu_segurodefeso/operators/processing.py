import csv
import re
from tempfile import NamedTemporaryFile
from zipfile import ZipFile
from typing import Any, Dict, Iterator, List
from elasticsearch.helpers import bulk
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook
from minio_plugin.hooks.minio_hook import MinioHook
from cgu_segurodefeso.operators.file_storage import MINIO_BUCKET, MINIO_CONN_ID, FILEPATH_KEY
from cgu_segurodefeso.operators.database import INDEX_KEY
from airflow.decorators import task


ELASTICSEARCH_CONN_ID = 'elasticsearch_default'
ELASTICSEARCH_MAX_RETRIES = 5
ELASTICSEARCH_TIMEOUT = 30
ELASTICSEARCH_MAX_CHUNK_SIZE = 100_000


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
    header = [
        'year_month_reference',
        'federative_unit',
        'siafi_county_code',
        'county',
        'cpf',
        'nis',
        'rgp',
        'name',
        'installment_value',
    ]

    reader = _open_zip(filepath=filepath, header=header)
    for i, line in enumerate(reader):
        person = {
            '_id': i,
            'year_month_reference': f"{line['year_month_reference'][:4]}-{line['year_month_reference'][4:]}",
            'federative_unit': line['federative_unit'].upper(),
            'siafi_county_code': line['siafi_county_code'].upper(),
            'county': line['county'],
            'cpf': re.sub(r'[^\d\*]', '', line['cpf']),
            'nis': line['nis'],
            'rgp': line['rgp'],
            'name': line['name'].upper(),
            'installment_value': round(float(re.sub(r',', '.', re.sub(r'\.', '', line['installment_value']))) * 100),
        }

        yield person


@task
def memory(file_data: Dict[str, str]) -> None:
    es = ElasticsearchHook(elasticsearch_conn_id=ELASTICSEARCH_CONN_ID)

    with es.get_conn() as conn:
        people = _load_people(filepath=file_data[FILEPATH_KEY])

        bulk(
            client=conn.es,
            actions=(
                {
                    '_index': file_data[INDEX_KEY],
                    '_id': str(person.pop('_id')),
                    '_source': person,
                }
                for person in people
            ),
            chunk_size=ELASTICSEARCH_MAX_CHUNK_SIZE,
            max_retries=ELASTICSEARCH_MAX_RETRIES,
            request_timeout=ELASTICSEARCH_TIMEOUT,
        )
