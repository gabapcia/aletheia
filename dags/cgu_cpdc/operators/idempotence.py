from typing import Tuple
from airflow.decorators import task
from minio_plugin.hooks.minio_hook import MinioHook
from cgu_cpdc.operators.file_storage import MINIO_BUCKET


@task(multiple_outputs=False)
def save_filedate(link: Tuple[str, str]) -> Tuple[str, str]:
    filedate = link[0]

    hook = MinioHook(conn_id='minio_default')
    minio = hook.get_client()

    objects = list(minio.list_objects(MINIO_BUCKET, prefix=f'/{filedate}', recursive=False))

    if len(objects) > 0:
        return tuple()

    return link
