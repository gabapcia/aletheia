from tempfile import NamedTemporaryFile
from zipfile import ZipFile
from typing import Dict, Tuple
from airflow.decorators import task
from minio_plugin.hooks.minio_hook import MinioHook
from minio_plugin.utils.file import HTTPFile
from minio_plugin.utils.wrapper import FileLike


MINIO_BUCKET = 'cgu-bpc'
MINIO_CONN_ID = 'minio_default'

ROOT_FOLDER_KEY = 'root_folder'
FILEDATE_KEY = 'date'
FILEPATH_KEY = 'path'


@task(multiple_outputs=False)
def download(link: Tuple[str, str]) -> Dict[str, str]:
    filedate = link[0]
    link = link[1]

    root_folder = f'/{filedate}'
    minio = MinioHook(conn_id=MINIO_CONN_ID)

    with HTTPFile(uri=link, timeout=2 * 60) as f:
        file = minio.save(reader=f, bucket=MINIO_BUCKET, folder=root_folder)

    return {
        FILEDATE_KEY: filedate,
        ROOT_FOLDER_KEY: root_folder,
        FILEPATH_KEY: file,
    }
