from airflow.exceptions import AirflowSkipException
from airflow.decorators import task
from minio_plugin.hooks.minio_hook import MinioHook
from cgu_pep.operators.file_storage import MINIO_BUCKET


@task
def save_filedate(filedate: str) -> None:
    hook = MinioHook(conn_id='minio_default')

    minio = hook.get_client()

    objects = list(minio.list_objects(MINIO_BUCKET, prefix=f'/{filedate}', recursive=False))
    if len(objects) > 0:
        raise AirflowSkipException
