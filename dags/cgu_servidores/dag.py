from datetime import timedelta
from pendulum import datetime
from pendulum.tz import timezone
from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup

from include.xcom_handler import drop_null, lookup_xcom
from cgu_servidores.operators.scraper import servidores, FILE_TYPES, HONORARY_KEY
from cgu_servidores.operators.idempotence import save_filedate
from cgu_servidores.operators.xcom_handler import get_files_by_type
from cgu_servidores.operators.file_storage import download, extract, delete_folder, ROOT_FOLDER_KEY
from cgu_servidores.operators.aggregation import name_files, join_named_files
from cgu_servidores.operators.database import elasticsearch
from cgu_servidores.operators.processing import spark


@dag(
    description='Servidores Civis e Militares do Executivo Federal da Controladoria-Geral da União',
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2022, 1, 1, tz=timezone('UTC')),
    schedule_interval='@daily',
    tags=['CGU', 'PF'],
    default_args={},
)
def cgu_servidores():
    links = servidores()

    idempotence = save_filedate.expand(links=links)

    links = drop_null.override(task_id='drop_files_already_downloaded')(data=idempotence)

    named_files_mapper = dict()
    extract_files_tasks = dict()
    for file_type in FILE_TYPES:
        with TaskGroup(group_id=f'{file_type}_files_download'):
            file_data = (
                get_files_by_type
                .override(task_id=f'get_{file_type}_files')(all_links=links, file_type=file_type)
            )

            downloaded_files = (
                download
                .override(
                    task_id=f'download_{file_type}',
                    retries=10,
                    retry_delay=timedelta(seconds=10),
                )
                .partial(filetype=file_type)
                .expand(file_data=file_data)
            )

            extracted_files = (
                extract
                .override(task_id=f'extract_{file_type}')
                .expand(download_data=drop_null.override(task_id='drop_missing_files')(data=downloaded_files))
            )
            extract_files_tasks[file_type] = extracted_files

            named_files = (
                name_files
                .override(task_id=f'name_{file_type}_files')
                .partial(filetype=file_type)
                .expand(files=extracted_files)
            )
            named_files_mapper[file_type] = named_files

    processing_groups = list()
    for file_type in [t for t in FILE_TYPES if t != HONORARY_KEY]:
        with TaskGroup(group_id=f'{file_type}_files_processing') as group:
            named_files = (
                join_named_files
                .override(task_id=f'join_{file_type}_named_files')
                .partial(honorary_files=named_files_mapper[HONORARY_KEY])
                .expand(named_file=named_files_mapper[file_type])
            )

            indexes_data = elasticsearch(named_files=named_files, filetype=file_type)
            spark(indices=indexes_data, filetype=file_type)

        processing_groups.append(group)

    with TaskGroup(group_id='cleanup') as cleanup:
        for file_type, extract_tasks in extract_files_tasks.items():
            extract_folders = (
                lookup_xcom
                .override(task_id=f'get_{file_type}_extract_folders')
                .partial(lookup=[ROOT_FOLDER_KEY])
                .expand(data=extract_tasks)
            )

            delete_folder(folders=extract_folders, filetype=file_type)

    processing_groups >> cleanup


# Register DAG
dag = cgu_servidores()
