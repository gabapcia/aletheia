from typing import Any, Dict, Tuple
from airflow.utils.task_group import TaskGroup
from minio_plugin.operators.download import DownloadFileOperator
from minio_plugin.utils.lookup import FolderLookup as MinioFolderLookup
from include.xcom_handler import lookup_xcom
from rfb_cnpj.operators.scraper import MAPPER_FILES, GENERATED_AT_KEY, PARTNER_KEY, BRANCH_KEY, COMPANY_KEY


MINIO_BUCKET = 'rfb-cnpj'


def download(links: Dict[str, Any]) -> Tuple[TaskGroup, Dict[str, Any]]:
    with TaskGroup(group_id='download') as download:
        minio_download_root_folder = MinioFolderLookup(links[GENERATED_AT_KEY], path='/{raw}')

        def minio_download_root_sub_folder(path: str) -> MinioFolderLookup:
            path = f'{minio_download_root_folder.path}/{path}'
            return MinioFolderLookup(links[GENERATED_AT_KEY], path=path)

        companies_zip = DownloadFileOperator.partial(
            task_id='companies',
            bucket=MINIO_BUCKET,
            minio_conn_id='minio_default',
            folder=minio_download_root_sub_folder('/company'),
        ).expand(uri=lookup_xcom.override(task_id='get_companies_uris')(links, [COMPANY_KEY]))

        branches_zip = DownloadFileOperator.partial(
            task_id='branches',
            bucket=MINIO_BUCKET,
            minio_conn_id='minio_default',
            folder=minio_download_root_sub_folder('/branch'),
        ).expand(uri=lookup_xcom.override(task_id='get_branches_uris')(links, [BRANCH_KEY]))

        partners_zip = DownloadFileOperator.partial(
            task_id='partners',
            bucket=MINIO_BUCKET,
            minio_conn_id='minio_default',
            folder=minio_download_root_sub_folder('/partner'),
        ).expand(uri=lookup_xcom.override(task_id='get_partners_uris')(links, [PARTNER_KEY]))

        tasks = {
            file: DownloadFileOperator(
                task_id=file,
                uri=links[file],
                bucket=MINIO_BUCKET,
                folder=minio_download_root_folder,
                minio_conn_id='minio_default',
            )
            for file in MAPPER_FILES
        }

    return download, {
        **tasks,
        COMPANY_KEY: companies_zip,
        BRANCH_KEY: branches_zip,
        PARTNER_KEY: partners_zip,
    }
