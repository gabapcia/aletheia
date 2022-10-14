from tempfile import NamedTemporaryFile
from zipfile import ZipFile
from typing import Dict, List, Union
from airflow.decorators import task
from minio_plugin.hooks.minio_hook import MinioHook
from minio_plugin.utils.file import HTTPFile
from minio_plugin.utils.wrapper import FileLike
from cgu_servidores.operators.scraper import FILEDATE_KEY, HONORARY_KEY


MINIO_BUCKET = 'cgu-servidores'
MINIO_CONN_ID = 'minio_default'

FILE_TYPE_KEY = 'filetype'
FILE_NAME_KEY = 'filename'
FILE_URI_KEY = 'uri'

ROOT_FOLDER_KEY = 'root_folder'
DOWNLOAD_PATH_KEY = 'zip_path'
EXTRACTED_FILES_KEY = 'files_extracted'

REGISTER_KEY = 'register'
SALARY_KEY = 'salary'
REMOVED_FROM_POSITION_KEY = 'removal'
OBSERVATION_KEY = 'observation'
HONORARY_ADVOCATIVE_KEY = 'honorary_advocative'
HONORARY_JETONS_KEY = 'honorary_jetons'

REGISTER_FILE_SUFFIX = 'Cadastro.csv'
SALARY_FILE_SUFFIX = 'Remuneracao.csv'
REMOVED_FROM_POSITION_FILE_SUFFIX = 'Afastamentos.csv'
OBSERVATION_FILE_SUFFIX = 'Observacoes.csv'
HONORARY_ADVOCATIVE_FILE_SUFFIX = 'Advocaticios.csv'
HONORARY_JETONS_FILE_SUFFIX = '(Jetons).csv'


@task(multiple_outputs=False)
def flat_catalog(catalog: Dict[str, Dict[str, Dict[str, str]]]) -> List[Dict[str, str]]:
    file_list = list()

    for reference_date, groupped_files in catalog.items():
        for filetype, files in groupped_files.items():
            for filename, uri in files.items():
                if uri:
                    data = {
                        FILEDATE_KEY: reference_date,
                        FILE_URI_KEY: uri,
                        FILE_NAME_KEY: filename,
                        FILE_TYPE_KEY: filetype,
                    }

                    file_list.append(data)

    return file_list


@task(multiple_outputs=False)
def group_files(file_list: List[Dict[str, Union[str, List[str]]]]) -> List[Dict[str, Union[str, Dict[str, str]]]]:
    honorary_files = dict()

    for file in file_list:
        if file[FILE_TYPE_KEY] != HONORARY_KEY:
            continue

        if file[FILEDATE_KEY] not in honorary_files:
            honorary_files[file[FILEDATE_KEY]] = dict()

        if file[EXTRACTED_FILES_KEY][0].endswith(HONORARY_ADVOCATIVE_FILE_SUFFIX):
            honorary_files[file[FILEDATE_KEY]][HONORARY_ADVOCATIVE_KEY] = file[EXTRACTED_FILES_KEY][0]
        elif file[EXTRACTED_FILES_KEY][0].endswith(HONORARY_JETONS_FILE_SUFFIX):
            honorary_files[file[FILEDATE_KEY]][HONORARY_JETONS_KEY] = file[EXTRACTED_FILES_KEY][0]

    catalog = list()
    for file in file_list:
        if file[FILE_TYPE_KEY] == HONORARY_KEY:
            continue

        data = {
            REGISTER_KEY: '',
            SALARY_KEY: '',
            REMOVED_FROM_POSITION_KEY: '',
            OBSERVATION_KEY: '',
        }

        for path in file[EXTRACTED_FILES_KEY]:
            if path.endswith(REGISTER_FILE_SUFFIX):
                data[REGISTER_KEY] = path
            elif path.endswith(SALARY_FILE_SUFFIX):
                data[SALARY_KEY] = path
            elif path.endswith(REMOVED_FROM_POSITION_FILE_SUFFIX):
                data[REMOVED_FROM_POSITION_KEY] = path
            elif path.endswith(OBSERVATION_FILE_SUFFIX):
                data[OBSERVATION_KEY] = path

        catalog.append({
            FILEDATE_KEY: file[FILEDATE_KEY],
            FILE_NAME_KEY: file[FILE_NAME_KEY],
            FILE_TYPE_KEY: file[FILE_TYPE_KEY],
            ROOT_FOLDER_KEY: file[ROOT_FOLDER_KEY],
            EXTRACTED_FILES_KEY: data,
            HONORARY_KEY: honorary_files.get(file[FILEDATE_KEY], {}),
        })

    return catalog


@task(multiple_outputs=False)
def download(data: Dict[str, str]) -> Dict[str, str]:
    root_folder = f'/{data[FILEDATE_KEY]}'

    minio = MinioHook(conn_id=MINIO_CONN_ID)

    with HTTPFile(
        uri=data[FILE_URI_KEY],
        name=f'{data[FILE_NAME_KEY]}.zip',
        timeout=2 * 60,
        size=-1,
    ) as file:
        zip_file = minio.save(reader=file, bucket=MINIO_BUCKET, folder=root_folder)

    data[ROOT_FOLDER_KEY] = root_folder
    data[DOWNLOAD_PATH_KEY] = zip_file

    return data


@task(multiple_outputs=False)
def extract(file_info: Dict[str, str]) -> Dict[str, Union[str, List[str]]]:
    folder = f'{file_info[ROOT_FOLDER_KEY]}/extracted/{file_info[FILE_TYPE_KEY]}/{file_info[FILE_NAME_KEY]}'

    minio = MinioHook(conn_id=MINIO_CONN_ID)

    with minio.get_object(bucket=MINIO_BUCKET, name=file_info[DOWNLOAD_PATH_KEY]) as f, NamedTemporaryFile(mode='wb') as tmp:
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

    file_info[EXTRACTED_FILES_KEY] = files
    file_info[ROOT_FOLDER_KEY] = folder

    return file_info


@task(multiple_outputs=False)
def delete_folder(folders: List[Dict[str, Union[str, Dict[str, str]]]]) -> None:
    minio = MinioHook(conn_id='minio_default')

    minio.delete_folder(bucket=MINIO_BUCKET, folder=folders[ROOT_FOLDER_KEY])
