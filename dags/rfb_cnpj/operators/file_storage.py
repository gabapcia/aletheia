from tempfile import NamedTemporaryFile
from zipfile import ZipFile
from typing import Dict, List, Union
from airflow.decorators import task
from minio_plugin.hooks.minio_hook import MinioHook
from minio_plugin.utils.file import HTTPFile
from minio_plugin.utils.wrapper import FileLike
from rfb_cnpj.operators.scraper import SINGLE_FILES, GENERATED_AT_KEY


MINIO_BUCKET = 'rfb-cnpj'
MINIO_CONN_ID = 'minio_default'

FILE_TYPE_KEY = 'filetype'
FILE_URI_KEY = 'uri'

ROOT_FOLDER_KEY = 'root_folder'
DOWNLOAD_PATH_KEY = 'zip_path'
EXTRACTED_FILE_KEY = 'file_extracted'


@task(multiple_outputs=False)
def flat_catalog(catalog: Dict[str, Union[str, List[str]]]) -> List[Dict[str, str]]:
    file_list = list()

    for filetype in filter(lambda c: c != GENERATED_AT_KEY, catalog.keys()):
        if filetype in SINGLE_FILES:
            data = {
                GENERATED_AT_KEY: catalog[GENERATED_AT_KEY],
                FILE_URI_KEY: catalog[filetype],
                FILE_TYPE_KEY: filetype,
            }

            file_list.append(data)
            continue

        for uri in catalog[filetype]:
            data = {
                GENERATED_AT_KEY: catalog[GENERATED_AT_KEY],
                FILE_URI_KEY: uri,
                FILE_TYPE_KEY: filetype,
            }

            file_list.append(data)

    return file_list


@task(multiple_outputs=False)
def download(data: Dict[str, str]) -> Dict[str, str]:
    root_folder = f'/{data[GENERATED_AT_KEY]}'

    minio = MinioHook(conn_id=MINIO_CONN_ID)

    with HTTPFile(uri=data[FILE_URI_KEY], timeout=2 * 60, size=-1) as file:
        zip_file = minio.save(reader=file, bucket=MINIO_BUCKET, folder=root_folder)

    data[ROOT_FOLDER_KEY] = root_folder
    data[DOWNLOAD_PATH_KEY] = zip_file

    return data


@task(multiple_outputs=False)
def extract(file_info: Dict[str, str]) -> Dict[str, str]:
    folder = f'{file_info[ROOT_FOLDER_KEY]}/extracted'

    if file_info[FILE_TYPE_KEY] not in SINGLE_FILES:
        folder = f'{folder}/{file_info[FILE_TYPE_KEY]}'

    minio = MinioHook(conn_id=MINIO_CONN_ID)

    with minio.get_object(bucket=MINIO_BUCKET, name=file_info[DOWNLOAD_PATH_KEY]) as f, NamedTemporaryFile(mode='wb') as tmp:
        while data := f.read(8 * 1024):
            tmp.write(data)

        tmp.flush()

        with ZipFile(tmp.name, mode='r') as zip:
            if len(zip.namelist()) > 1:
                raise Exception('Files not mapped')

            filename = zip.namelist()[0]
            with zip.open(filename, mode='r') as file:
                path = minio.save(reader=FileLike(file, name=filename), bucket=MINIO_BUCKET, folder=folder)

                if not path.startswith('/'):
                    path = f'/{path}'

    file_info[EXTRACTED_FILE_KEY] = path
    file_info[ROOT_FOLDER_KEY] = folder

    return file_info


@task(multiple_outputs=False)
def delete_folder(catalog: List[Dict[str, str]]) -> None:
    for c in catalog:
        if c[FILE_TYPE_KEY] in SINGLE_FILES:
            folder = c[ROOT_FOLDER_KEY]
            break

    minio = MinioHook(conn_id='minio_default')
    minio.delete_folder(bucket=MINIO_BUCKET, folder=folder)
