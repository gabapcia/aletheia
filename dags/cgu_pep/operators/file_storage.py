from typing import Dict
from airflow.exceptions import AirflowSkipException
from airflow.decorators import task
from minio_plugin.hooks.minio_hook import MinioHook
from minio_plugin.utils.file import HTTPFile
from cgu_pep.operators.scraper import GENERATED_AT_KEY, FILE_URI_KEY


MINIO_BUCKET = 'cgu-pep'
MINIO_CONN_ID = 'minio_default'


@task(multiple_outputs=False)
def idempotence(info: Dict[str, str]) -> None:
    folder = f'/{info[GENERATED_AT_KEY]}'

    hook = MinioHook(conn_id=MINIO_CONN_ID)
    minio = hook.get_client()

    objects = list(minio.list_objects(MINIO_BUCKET, prefix=folder, recursive=False))
    if len(objects) > 0:
        raise AirflowSkipException


@task(multiple_outputs=False)
def download(info: Dict[str, str]) -> str:
    folder = f'/{info[GENERATED_AT_KEY]}'

    minio = MinioHook(conn_id=MINIO_CONN_ID)
    with HTTPFile(uri=info[FILE_URI_KEY], timeout=2 * 60, size=-1) as f:
        file = minio.save(reader=f, bucket=MINIO_BUCKET, folder=folder)

    return file
