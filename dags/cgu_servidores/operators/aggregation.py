from typing import Dict, List, Union
from airflow.decorators import task
from cgu_servidores.operators.scraper import HONORARY_KEY
from cgu_servidores.operators.file_storage import FILEDATE_KEY, ROOT_FOLDER_KEY, EXTRACTED_FILES_KEY, FILE_SUBTYPE_KEY


REGISTER_FILE_SUFFIX = 'Cadastro.csv'
SALARY_FILE_SUFFIX = 'Remuneracao.csv'
REMOVED_FROM_POSITION_FILE_SUFFIX = 'Afastamentos.csv'
OBSERVATION_FILE_SUFFIX = 'Observacoes.csv'
HONORARY_ADVOCATIVE_FILE_SUFFIX = 'Advocaticios.csv'
HONORARY_JETONS_FILE_SUFFIX = '(Jetons).csv'

REGISTER_KEY = 'register'
SALARY_KEY = 'salary'
REMOVED_FROM_POSITION_KEY = 'removal'
OBSERVATION_KEY = 'observation'
HONORARY_ADVOCATIVE_KEY = 'honorary_advocative'
HONORARY_JETONS_KEY = 'honorary_jetons'


def _build_files_map(paths: List[str], filetype: str) -> Dict[str, str]:
    if filetype == HONORARY_KEY:
        if paths[0].endswith(HONORARY_ADVOCATIVE_FILE_SUFFIX):
            data = {HONORARY_ADVOCATIVE_KEY: paths[0]}
        elif paths[0].endswith(HONORARY_JETONS_FILE_SUFFIX):
            data = {HONORARY_JETONS_KEY: paths[0]}

        return data

    data = {
        REGISTER_KEY: '',
        SALARY_KEY: '',
        REMOVED_FROM_POSITION_KEY: '',
        OBSERVATION_KEY: '',
    }

    for path in paths:
        if path.endswith(REGISTER_FILE_SUFFIX):
            data[REGISTER_KEY] = path
        elif path.endswith(SALARY_FILE_SUFFIX):
            data[SALARY_KEY] = path
        elif path.endswith(REMOVED_FROM_POSITION_FILE_SUFFIX):
            data[REMOVED_FROM_POSITION_KEY] = path
        elif path.endswith(OBSERVATION_FILE_SUFFIX):
            data[OBSERVATION_KEY] = path

    return data


@task(multiple_outputs=False)
def name_files(files: Dict[str, Union[str, List[str]]], filetype: str) -> Dict[str, str]:
    joined_data = {
        FILE_SUBTYPE_KEY: files[FILE_SUBTYPE_KEY],
        FILEDATE_KEY: files[FILEDATE_KEY],
        ROOT_FOLDER_KEY: files[ROOT_FOLDER_KEY],
        **_build_files_map(paths=files[EXTRACTED_FILES_KEY], filetype=filetype),
    }

    return joined_data


@task(multiple_outputs=False)
def join_named_files(named_file: Dict[str, str], honorary_files: List[Dict[str, str]]) -> Dict[str, str]:
    named_file[HONORARY_ADVOCATIVE_KEY] = ''
    named_file[HONORARY_JETONS_KEY] = ''

    for honorary_file in filter(lambda f: f[FILEDATE_KEY] == named_file[FILEDATE_KEY], honorary_files):
        if HONORARY_ADVOCATIVE_KEY in honorary_file:
            named_file[HONORARY_ADVOCATIVE_KEY] = honorary_file[HONORARY_ADVOCATIVE_KEY]
        elif HONORARY_JETONS_KEY in honorary_file:
            named_file[HONORARY_JETONS_KEY] = honorary_file[HONORARY_JETONS_KEY]

    return named_file
