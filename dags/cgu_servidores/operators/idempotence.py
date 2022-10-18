from typing import Dict
from airflow.decorators import task
from minio_plugin.hooks.minio_hook import MinioHook
from cgu_servidores.operators.file_storage import MINIO_BUCKET


@task(multiple_outputs=False)
def idempotence(catalog: Dict[str, Dict[str, Dict[str, str]]]) -> Dict[str, Dict[str, Dict[str, str]]]:
    hook = MinioHook(conn_id='minio_default')
    minio = hook.get_client()

    cleaned_catalog = dict()

    for filedate in catalog.keys():
        objects = list(minio.list_objects(MINIO_BUCKET, prefix=f'/{filedate}', recursive=False))

        if len(objects) > 0:
            continue

        cleaned_catalog[filedate] = catalog[filedate]

    return cleaned_catalog
