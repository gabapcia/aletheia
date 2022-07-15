from airflow.decorators import task
from minio_plugin.hooks.minio_hook import MinioHook
from rfb_cnpj.operators.download import MINIO_BUCKET


class AlreadyExists(Exception):
    def __init__(self, filedate: str) -> None:
        self.filedate = filedate

    def __str__(self) -> str:
        return f'"{self.filedate}" already exists'


@task
def save_filedate(filedate: str) -> None:
    hook = MinioHook(conn_id='minio_default')

    minio = hook.get_client()

    objects = list(minio.list_objects(MINIO_BUCKET, prefix=f'/{filedate}', recursive=False))
    if len(objects) > 0:
        raise AlreadyExists(filedate=filedate)
