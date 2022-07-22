from typing import Dict, Tuple, Union
from airflow.decorators import task
from minio_plugin.hooks.minio_hook import MinioHook
from cgu_bolsafamilia.operators.storage import MINIO_BUCKET


@task(multiple_outputs=False)
def save_filedate(links: Tuple[str, Dict[str, str]]) -> Union[Tuple[str, Dict[str, str]], None]:
    filedate = links[0]

    hook = MinioHook(conn_id='minio_default')
    minio = hook.get_client()

    objects = list(minio.list_objects(MINIO_BUCKET, prefix=f'/{filedate}', recursive=False))

    if len(objects) > 0:
        return dict()

    return links
