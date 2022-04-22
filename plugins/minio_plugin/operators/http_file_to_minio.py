from typing import Union
from airflow.utils.context import Context
from airflow.models.baseoperator import BaseOperator
from minio_plugin.hooks.minio_hook import MinioHook
from minio_plugin.http.file import HTTPFile
from minio_plugin.utils.lookup import XComArgLookup, FolderLookup


class FileDownloadOperator(BaseOperator):
    def __init__(
        self,
        uri: Union[XComArgLookup, str],
        folder: Union[FolderLookup, str] = '',
        minio_conn_id: str = 'minio_default',
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)

        self.uri = uri
        self.folder = folder
        self.minio_conn_id = minio_conn_id

    def execute(self, context: Context) -> str:
        if isinstance(self.uri, XComArgLookup):
            self.uri = self.uri.get(context)

        if isinstance(self.folder, FolderLookup):
            self.folder = self.folder.get(context)

        minio = MinioHook(conn_id=self.minio_conn_id)
        with HTTPFile(uri=self.uri) as file:
            filename = minio.save(file, folder=self.folder)

        return filename
