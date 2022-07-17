from typing import Any, Dict, Tuple
from airflow.utils.task_group import TaskGroup
from minio_plugin.operators.download import DownloadFileOperator
from minio_plugin.operators.extract_file import ExtractFileOperator
from minio_plugin.utils.lookup import FolderLookup as MinioFolderLookup
from cgu_pep.operators.scraper import GENERATED_AT_KEY, FILE_URI_KEY


MINIO_BUCKET = 'cgu-pep'

ROOT_FOLDER_KEY = 'root_folder'
TASK_KEY = 'task'


def download(links: Dict[str, Any]) -> Tuple[TaskGroup, Dict[str, Any]]:
    with TaskGroup(group_id='download') as download:
        folder = MinioFolderLookup(links[GENERATED_AT_KEY], path='/{raw}')

        task = DownloadFileOperator(
            task_id='peps',
            bucket=MINIO_BUCKET,
            minio_conn_id='minio_default',
            folder=MinioFolderLookup(links[GENERATED_AT_KEY], path='/{raw}'),
            uri=links[FILE_URI_KEY],
        )

    return download, {
        ROOT_FOLDER_KEY: folder,
        TASK_KEY: task,
    }


def extract(links: Dict[str, Any], download_task: DownloadFileOperator) -> Tuple[TaskGroup, Dict[str, Any]]:
    with TaskGroup(group_id='extract') as extract:
        folder = MinioFolderLookup(links[GENERATED_AT_KEY], path='/{raw}/extracted')

        task = ExtractFileOperator(
            task_id='peps',
            bucket=MINIO_BUCKET,
            minio_conn_id='minio_default',
            folder=folder,
            zip=download_task.output,
        )

    return extract, {
        ROOT_FOLDER_KEY: folder,
        TASK_KEY: task,
    }
