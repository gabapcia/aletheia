from tempfile import NamedTemporaryFile
from zipfile import ZipFile
from typing import Dict, List, Tuple, Union
from airflow.decorators import task
from minio_plugin.hooks.minio_hook import MinioHook
from minio_plugin.operators.delete_folder import DeleteFolderOperator
from minio_plugin.utils.file import HTTPFile
from minio_plugin.utils.wrapper import FileLike
from cgu_servidores.operators.xcom_handler import (
    FILEDATE_KEY as SCRAPER_FILEDATE_KEY,
    FILENAME_KEY,
    FILE_URI_KEY,
    FILE_SUBTYPE_KEY,
)


MINIO_BUCKET = 'cgu-servidores'
MINIO_CONN_ID = 'minio_default'

ROOT_FOLDER_KEY = 'root_folder'
FILEDATE_KEY = 'date'

DOWNLOAD_PATH_KEY = 'path'
EXTRACTED_FILES_KEY = 'paths'


@task(multiple_outputs=False)
def download(filetype: str, file_data: Dict[str, str]) -> Dict[str, Union[str, Tuple[str, str]]]:
    filedate = file_data[SCRAPER_FILEDATE_KEY]
    filename = file_data[FILENAME_KEY]
    link = file_data[FILE_URI_KEY]

    if not link:
        return dict()

    root_folder = f'/{filedate}/{filetype}'

    minio = MinioHook(conn_id=MINIO_CONN_ID)

    with HTTPFile(uri=link, timeout=2 * 60, size=-1) as file:
        zip_file = minio.save(reader=file, bucket=MINIO_BUCKET, folder=root_folder)

    return {
        FILEDATE_KEY: filedate,
        ROOT_FOLDER_KEY: root_folder,
        FILE_SUBTYPE_KEY: file_data[FILE_SUBTYPE_KEY],
        DOWNLOAD_PATH_KEY: (filename, zip_file),
    }


@task(multiple_outputs=False)
def extract(download_data: Dict[str, Union[str, Tuple[str, str]]]) -> Dict[str, Union[str, List[str]]]:
    filename = download_data[DOWNLOAD_PATH_KEY][0]
    path = download_data[DOWNLOAD_PATH_KEY][1]
    folder = f'{download_data[ROOT_FOLDER_KEY]}/extracted/{filename}'

    extracted_files = {
        FILEDATE_KEY: download_data[FILEDATE_KEY],
        FILE_SUBTYPE_KEY: download_data[FILE_SUBTYPE_KEY],
        ROOT_FOLDER_KEY: folder,
    }

    minio = MinioHook(conn_id=MINIO_CONN_ID)

    with minio.get_object(bucket=MINIO_BUCKET, name=path) as f, NamedTemporaryFile(mode='wb') as tmp:
        while data := f.read(8 * 1024):
            tmp.write(data)

        tmp.flush()

        files = []
        with ZipFile(tmp.name, mode='r') as zip:
            for filename in zip.namelist():
                with zip.open(filename, mode='r') as file:
                    path = minio.save(reader=FileLike(file, name=filename), bucket=MINIO_BUCKET, folder=folder)

                    if not path.startswith('/'):
                        path = f'/{path}'

                    files.append(path)

        extracted_files[EXTRACTED_FILES_KEY] = files

    return extracted_files


def delete_folder(folders: List[str], filetype: str) -> DeleteFolderOperator:
    task = (
        DeleteFolderOperator
        .partial(
            task_id=f'delete_{filetype}_folders',
            bucket=MINIO_BUCKET,
            minio_conn_id=MINIO_CONN_ID,
        )
        .expand(folder=folders)
    )

    return task
