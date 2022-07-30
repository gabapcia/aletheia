from tempfile import NamedTemporaryFile
from typing import Dict, List, Tuple
from zipfile import ZipFile
from airflow.decorators import task
from minio_plugin.hooks.minio_hook import MinioHook
from minio_plugin.utils.file import HTTPFile
from minio_plugin.utils.wrapper import FileLike


MINIO_BUCKET = 'cgu-nfe'
MINIO_CONN_ID = 'minio_default'

ROOT_FOLDER_KEY = 'root_folder'
FILEDATE_KEY = 'date'
FILEPATH_KEY = 'path'
NFE_FILEPATH_KEY = 'nfe_path'
EVENTS_FILEPATH_KEY = 'events_path'
ITEMS_FILEPATH_KEY = 'items_path'

NFE_FILEPATH_SUFFIX = 'NotaFiscal.csv'
EVENTS_FILEPATH_SUFFIX = 'NotaFiscalEvento.csv'
ITEMS_FILEPATH_SUFFIX = 'NotaFiscalItem.csv'


@task(multiple_outputs=False)
def download(link: Tuple[str, str]) -> Dict[str, str]:
    filedate = link[0]
    link = link[1]

    root_folder = f'/{filedate}'
    minio = MinioHook(conn_id=MINIO_CONN_ID)

    with HTTPFile(uri=link, timeout=2 * 60, size=-1) as f:
        file = minio.save(reader=f, bucket=MINIO_BUCKET, folder=root_folder)

    return {
        FILEDATE_KEY: filedate,
        ROOT_FOLDER_KEY: root_folder,
        FILEPATH_KEY: file,
    }


@task(multiple_outputs=False)
def extract(download_zips: Dict[str, str]) -> Dict[str, str]:
    path = download_zips[FILEPATH_KEY]
    folder = f'{download_zips[ROOT_FOLDER_KEY]}/extracted'

    extracted_files = {
        FILEDATE_KEY: download_zips[FILEDATE_KEY],
        ROOT_FOLDER_KEY: folder,
    }

    minio = MinioHook(conn_id='minio_default')

    with minio.get_object(bucket=MINIO_BUCKET, name=path) as f, NamedTemporaryFile(mode='wb') as tmp:
        while data := f.read(8 * 1024):
            tmp.write(data)

        tmp.flush()

        files: List[str] = []
        with ZipFile(tmp.name, mode='r') as zip:
            for filename in zip.namelist():
                with zip.open(filename, mode='r') as file:
                    path = minio.save(reader=FileLike(file), bucket=MINIO_BUCKET, folder=folder)

                    if path.startswith('/'):
                        path = path[1:]

                    files.append(path)

    nfe_filepath = list(filter(lambda p: p.endswith(NFE_FILEPATH_SUFFIX), files))[0]
    events_filepath = list(filter(lambda p: p.endswith(EVENTS_FILEPATH_SUFFIX), files))[0]
    items_filepath = list(filter(lambda p: p.endswith(ITEMS_FILEPATH_SUFFIX), files))[0]

    extracted_files.update({
        NFE_FILEPATH_KEY: nfe_filepath,
        EVENTS_FILEPATH_KEY: events_filepath,
        ITEMS_FILEPATH_KEY: items_filepath,
    })

    return extracted_files


@task(multiple_outputs=False)
def delete_folder(folders: Dict[str, str]) -> None:
    minio = MinioHook(conn_id='minio_default')

    minio.delete_folder(bucket=MINIO_BUCKET, folder=folders[ROOT_FOLDER_KEY])
