from tempfile import NamedTemporaryFile
from typing import Any, Dict, List
from zipfile import ZipFile
from airflow.decorators import task
from minio_plugin.hooks.minio_hook import MinioHook
from minio_plugin.utils.file import HTTPFile
from minio_plugin.utils.wrapper import FileLike
from tse_candidatos.operators.scraper import FILE_URI_KEY


MINIO_BUCKET = 'tse-candidatos'
MINIO_CONN_ID = 'minio_default'

ROOT_FOLDER_KEY = 'folder'
FILES_DATE_KEY = 'date'
REFERENCE_YEAR_KEY = 'year'
FILE_TYPE_KEY = 'file_type'
FILE_PATH_KEY = 'file'


@task(multiple_outputs=False)
def flat_catalog(catalog: Dict[str, Dict[str, Any]]) -> List[Dict[str, str]]:
    file_list = list()

    for year, files in catalog.items():
        files_date = files.pop(FILES_DATE_KEY)
        for filetype, info in files.items():
            data = {
                REFERENCE_YEAR_KEY: year,
                FILES_DATE_KEY: files_date,
                FILE_URI_KEY: info[FILE_URI_KEY],
                FILE_TYPE_KEY: filetype,
            }

            file_list.append(data)

    return file_list


@task(multiple_outputs=False)
def group_files(file_list: List[Dict[str, str]]) -> List[Dict[str, str]]:
    groupped_files = dict()

    for file in file_list:
        data = groupped_files.get(file[REFERENCE_YEAR_KEY], {
            REFERENCE_YEAR_KEY: file[REFERENCE_YEAR_KEY],
            FILES_DATE_KEY: file[FILES_DATE_KEY],
        })

        data[file[FILE_TYPE_KEY]] = file[ROOT_FOLDER_KEY]

        groupped_files[file[REFERENCE_YEAR_KEY]] = data

    return list(groupped_files.values())


@task(multiple_outputs=False)
def download(data: Dict[str, str]) -> Dict[str, str]:
    root_folder = f'/{data[REFERENCE_YEAR_KEY]}/{data[FILES_DATE_KEY]}'

    minio = MinioHook(conn_id=MINIO_CONN_ID)

    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
    }

    with HTTPFile(
        uri=data.pop(FILE_URI_KEY),
        name=f'{data[FILE_TYPE_KEY]}.zip',
        timeout=2 * 60,
        size=-1,
        use_headers=headers,
    ) as f:
        file = minio.save(reader=f, bucket=MINIO_BUCKET, folder=root_folder)

    data[FILE_PATH_KEY] = file
    data[ROOT_FOLDER_KEY] = root_folder

    return data


@task(multiple_outputs=False)
def extract(file_info: Dict[str, str]) -> Dict[str, str]:
    folder = f'{file_info[ROOT_FOLDER_KEY]}/extracted/{file_info[FILE_TYPE_KEY]}'

    minio = MinioHook(conn_id=MINIO_CONN_ID)

    with minio.get_object(bucket=MINIO_BUCKET, name=file_info[FILE_PATH_KEY]) as f, NamedTemporaryFile(mode='wb') as tmp:
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

    file_info[FILE_PATH_KEY] = files
    file_info[ROOT_FOLDER_KEY] = folder

    return file_info


@task(multiple_outputs=False)
def delete_folder(folders: List[Dict[str, str]]) -> None:
    minio = MinioHook(conn_id='minio_default')

    minio.delete_folder(bucket=MINIO_BUCKET, folder=folders[ROOT_FOLDER_KEY])
