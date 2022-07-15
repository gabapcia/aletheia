from typing import Union
from airflow import XComArg
from airflow.utils.context import Context
from airflow.models.baseoperator import BaseOperator
from minio_plugin.hooks.minio_hook import MinioHook
from minio_plugin.utils.file import HTTPFile
from minio_plugin.utils.lookup import FolderLookup


class DownloadFileOperator(BaseOperator):
    def __init__(
        self,
        uri: Union[XComArg, str],
        bucket: str,
        folder: Union[FolderLookup, str] = '',
        minio_conn_id: str = 'minio_default',
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)

        self.uri = uri
        self.bucket = bucket
        self.folder = folder
        self.minio_conn_id = minio_conn_id

    def execute(self, context: Context) -> str:
        if isinstance(self.uri, XComArg):
            self.uri = self.uri.resolve(context)

        if isinstance(self.folder, FolderLookup):
            self.folder = self.folder.resolve(context)

        minio = MinioHook(conn_id=self.minio_conn_id)
        with HTTPFile(uri=self.uri) as file:
            filename = minio.save(reader=file, bucket=self.bucket, folder=self.folder)

        return filename
