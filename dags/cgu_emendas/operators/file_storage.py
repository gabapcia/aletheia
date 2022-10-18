from airflow.decorators import task
from minio_plugin.hooks.minio_hook import MinioHook
from minio_plugin.utils.file import HTTPFile


MINIO_BUCKET = 'cgu-emendas'
MINIO_CONN_ID = 'minio_default'


@task(multiple_outputs=False)
def download(uri: str) -> str:
    minio = MinioHook(conn_id=MINIO_CONN_ID)
    with HTTPFile(uri=uri, timeout=2 * 60, size=-1) as f:
        file = minio.save(reader=f, bucket=MINIO_BUCKET, folder='/')

    return file
