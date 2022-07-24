from typing import Any, Dict
from minio_plugin.operators.download import DownloadFileOperator
from minio_plugin.operators.extract_file import ExtractFileOperator
from minio_plugin.operators.delete_folder import DeleteFolderOperator
from minio_plugin.utils.lookup import FolderLookup as MinioFolderLookup
from cgu_pep.operators.scraper import GENERATED_AT_KEY, FILE_URI_KEY


MINIO_BUCKET = 'cgu'

ROOT_FOLDER_KEY = 'root_folder'
TASK_KEY = 'task'


def download(links: Dict[str, Any]) -> Dict[str, Any]:
    folder = MinioFolderLookup(links[GENERATED_AT_KEY], path='/{raw}')

    task = DownloadFileOperator(
        task_id='download',
        bucket=MINIO_BUCKET,
        minio_conn_id='minio_default',
        folder=MinioFolderLookup(links[GENERATED_AT_KEY], path='/{raw}'),
        uri=links[FILE_URI_KEY],
    )

    return {
        ROOT_FOLDER_KEY: folder,
        TASK_KEY: task,
    }


def extract(links: Dict[str, Any], download_task: DownloadFileOperator) -> Dict[str, Any]:
    folder = MinioFolderLookup(links[GENERATED_AT_KEY], path='/{raw}/extracted')

    task = ExtractFileOperator(
        task_id='extract',
        bucket=MINIO_BUCKET,
        minio_conn_id='minio_default',
        folder=folder,
        zip=download_task.output,
    )

    return {
        ROOT_FOLDER_KEY: folder,
        TASK_KEY: task,
    }


def delete_files(extract_folder: MinioFolderLookup) -> DeleteFolderOperator:
    delete_extracted_files = DeleteFolderOperator(
        task_id='delete_files',
        bucket=MINIO_BUCKET,
        folder=extract_folder,
        minio_conn_id='minio_default',
    )

    return delete_extracted_files
