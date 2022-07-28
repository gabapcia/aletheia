import csv
from datetime import datetime
import re
from tempfile import NamedTemporaryFile
from zipfile import ZipFile
from typing import Any, Dict, Iterator, List
from elasticsearch.helpers import bulk
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook
from minio_plugin.hooks.minio_hook import MinioHook
from cgu_cpdc.operators.file_storage import MINIO_BUCKET, MINIO_CONN_ID, FILEPATH_KEY
from cgu_cpdc.operators.database import INDEX_KEY
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


def _load_transactions(filepath: str) -> Iterator[Dict[str, Any]]:
    # Source: https://www.portaldatransparencia.gov.br/pagina-interna/603411-dicionario-de-dados-cpdc
    header = [
        'higher_agency_code',
        'higher_agency_name',
        'agency_code',
        'agency_name',
        'management_unit_code',
        'management_unit_name',
        'statement_year',
        'statement_month',
        'card_holder_cpf',
        'card_holder_name',
        'beneficiary_tax_id',
        'beneficiary_name',
        'expense_performer',
        'agreement_number',
        'convenient_code',
        'convenor_name',
        'transfer',
        'transaction',
        'transaction_date',
        'transaction_value',
    ]

    reader = _open_zip(filepath=filepath, header=header)
    for i, line in enumerate(reader):
        transaction = {
            '_id': i,
            'higher_agency_code': line['higher_agency_code'],
            'higher_agency_name': line['higher_agency_name'].strip().upper(),
            'agency_code': line['agency_code'],
            'agency_name': line['agency_name'].strip().upper(),
            'management_unit_code': line['management_unit_code'],
            'management_unit_name': line['management_unit_name'].strip().upper(),
            'statement_year_month': f"{line['statement_year'][:4]}-{line['statement_month'][4:]}",
            'card_holder_cpf': re.sub(r'[^\d\*]', '', line['card_holder_cpf']),
            'card_holder_name': line['card_holder_name'].strip().upper(),
            'beneficiary_tax_id': re.sub(r'[^\d\*]', '', line['beneficiary_tax_id']),
            'beneficiary_name': line['beneficiary_name'].strip().upper(),
            'expense_performer': line['expense_performer'],
            'agreement_number': line['agreement_number'],
            'convenient_code': line['convenient_code'],
            'convenor_name': line['convenor_name'].strip().upper(),
            'transfer': line['transfer'],
            'transaction': line['transaction'],
            'transaction_date': (
                datetime.strptime(line['transaction_date'], '%d/%m/%Y')
                if line['transaction_date'].strip()
                else None
            ),
            'transaction_value': round(float(re.sub(r',', '.', re.sub(r'\.', '', line['transaction_value']))) * 100),
        }

        yield transaction


@task
def memory(file_data: Dict[str, str]) -> None:
    es = ElasticsearchHook(elasticsearch_conn_id=ELASTICSEARCH_CONN_ID)

    with es.get_conn() as conn:
        transactions = _load_transactions(filepath=file_data[FILEPATH_KEY])

        bulk(
            client=conn.es,
            actions=(
                {
                    '_index': file_data[INDEX_KEY],
                    '_id': str(transaction.pop('_id')),
                    '_source': transaction,
                }
                for transaction in transactions
            ),
            chunk_size=ELASTICSEARCH_MAX_CHUNK_SIZE,
            max_retries=ELASTICSEARCH_MAX_RETRIES,
            request_timeout=ELASTICSEARCH_TIMEOUT,
        )
