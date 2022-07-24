from typing import Any, Dict
from airflow.decorators import task
from minio_plugin.hooks.minio_hook import MinioHook
from cgu_servidores.operators.scraper import FILEDATE_KEY
from cgu_servidores.operators.file_storage import MINIO_BUCKET


@task(multiple_outputs=False)
def save_filedate(links: Dict[str, Any]) -> Dict[str, Any]:
    filedate = links[FILEDATE_KEY]

    hook = MinioHook(conn_id='minio_default')
    minio = hook.get_client()

    objects = list(minio.list_objects(MINIO_BUCKET, prefix=f'/{filedate}', recursive=False))

    if len(objects) > 0:
        return dict()

    return links
