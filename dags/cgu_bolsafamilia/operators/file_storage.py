from tempfile import NamedTemporaryFile
from zipfile import ZipFile
from typing import Dict, Tuple
from airflow.decorators import task
from minio_plugin.hooks.minio_hook import MinioHook
from minio_plugin.utils.file import HTTPFile
from minio_plugin.utils.wrapper import FileLike
from cgu_bolsafamilia.operators.scraper import PAYMENT_KEY, WITHDRAW_KEY


MINIO_BUCKET = 'cgu-bolsafamilia'

ROOT_FOLDER_KEY = 'root_folder'
FILEDATE_KEY = 'date'


@task(multiple_outputs=False)
def download(links: Tuple[str, Dict[str, str]]) -> Dict[str, str]:
    filedate = links[0]
    links = links[1]

    root_folder = f'/{filedate}'
    minio = MinioHook(conn_id='minio_default')

    with HTTPFile(uri=links[PAYMENT_KEY], name=f'{PAYMENT_KEY}.zip', timeout=2 * 60, size=-1) as payment_file:
        payment_zip = minio.save(reader=payment_file, bucket=MINIO_BUCKET, folder=root_folder)

    with HTTPFile(uri=links[WITHDRAW_KEY], name=f'{WITHDRAW_KEY}.zip', timeout=2 * 60, size=-1) as withdraw_file:
        withdraw_zip = minio.save(reader=withdraw_file, bucket=MINIO_BUCKET, folder=root_folder)

    return {
        FILEDATE_KEY: filedate,
        ROOT_FOLDER_KEY: root_folder,
        PAYMENT_KEY: payment_zip,
        WITHDRAW_KEY: withdraw_zip,
    }


@task(multiple_outputs=False)
def extract(download_zips: Dict[str, str]) -> Dict[str, str]:
    minio = MinioHook(conn_id='minio_default')
    folder = f'{download_zips[ROOT_FOLDER_KEY]}/extracted'

    extracted_files = {
        FILEDATE_KEY: download_zips[FILEDATE_KEY],
        ROOT_FOLDER_KEY: folder,
    }

    for key in [PAYMENT_KEY, WITHDRAW_KEY]:
        path = download_zips[key]

        with minio.get_object(bucket=MINIO_BUCKET, name=path) as f, NamedTemporaryFile(mode='wb') as tmp:
            while data := f.read(8 * 1024):
                tmp.write(data)

            tmp.flush()

            files = []
            with ZipFile(tmp.name, mode='r') as zip:
                for filename in zip.namelist():
                    with zip.open(filename, mode='r') as file:
                        path = minio.save(reader=FileLike(file, name=f'{key}.csv'), bucket=MINIO_BUCKET, folder=folder)
                        files.append(path)

        extracted_files[key] = files

    return extracted_files


@task(multiple_outputs=False)
def delete_folder(folders: Dict[str, str]) -> None:
    minio = MinioHook(conn_id='minio_default')

    minio.delete_folder(bucket=MINIO_BUCKET, folder=folders[ROOT_FOLDER_KEY])
