from typing import Dict, List, Union
from airflow.exceptions import AirflowSkipException
from airflow.decorators import task
from minio_plugin.hooks.minio_hook import MinioHook
from rfb_cnpj.operators.scraper import GENERATED_AT_KEY
from rfb_cnpj.operators.file_storage import MINIO_BUCKET


@task(multiple_outputs=False)
def idempotence(data: Dict[str, Union[List[str], str]]) -> None:
    hook = MinioHook(conn_id='minio_default')

    minio = hook.get_client()

    objects = list(minio.list_objects(MINIO_BUCKET, prefix=f'/{data[GENERATED_AT_KEY]}', recursive=False))
    if len(objects) > 0:
        raise AirflowSkipException
