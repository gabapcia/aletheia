from typing import Any, Dict, Tuple
from airflow import XComArg
from airflow.utils.task_group import TaskGroup
from minio_plugin.operators.extract_file import ExtractFileOperator
from minio_plugin.utils.lookup import FolderLookup as MinioFolderLookup
from rfb_cnpj.operators.scraper import MAPPER_FILES, GENERATED_AT_KEY, COMPANY_KEY, BRANCH_KEY, PARTNER_KEY
from rfb_cnpj.operators.download import MINIO_BUCKET


ROOT_FOLDER_KEY = 'root_folder'


def extract(links: Dict[str, Any], downloads: Dict[str, Any]) -> Tuple[TaskGroup, Dict[str, Any]]:
    with TaskGroup(group_id='extract') as extract:
        minio_extract_root_folder = MinioFolderLookup(links[GENERATED_AT_KEY], path='/{raw}/extracted')

        def minio_extract_root_sub_folder(path: str):
            path = f'{minio_extract_root_folder.path}/{path}'
            return MinioFolderLookup(links[GENERATED_AT_KEY], path=path)

        companies_files = ExtractFileOperator.partial(
            task_id='companies',
            bucket=MINIO_BUCKET,
            minio_conn_id='minio_default',
            folder=minio_extract_root_sub_folder('/company'),
        ).expand(zip=XComArg(downloads[COMPANY_KEY]))

        branches_files = ExtractFileOperator.partial(
            task_id='branches',
            bucket=MINIO_BUCKET,
            minio_conn_id='minio_default',
            folder=minio_extract_root_sub_folder('/branch'),
        ).expand(zip=XComArg(downloads[BRANCH_KEY]))

        partners_files = ExtractFileOperator.partial(
            task_id='partners',
            bucket=MINIO_BUCKET,
            minio_conn_id='minio_default',
            folder=minio_extract_root_sub_folder('/partner'),
        ).expand(zip=XComArg(downloads[PARTNER_KEY]))

        tasks = {
            file: ExtractFileOperator(
                task_id=file,
                bucket=MINIO_BUCKET,
                minio_conn_id='minio_default',
                folder=minio_extract_root_folder,
                zip=downloads[file].output,
            )
            for file in MAPPER_FILES
        }

    return extract, {
        **tasks,
        ROOT_FOLDER_KEY: minio_extract_root_folder,
        COMPANY_KEY: companies_files,
        BRANCH_KEY: branches_files,
        PARTNER_KEY: partners_files,
    }
