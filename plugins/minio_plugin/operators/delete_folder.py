from typing import Union
from airflow.utils.context import Context
from airflow.models.baseoperator import BaseOperator
from minio_plugin.hooks.minio_hook import MinioHook
from minio_plugin.utils.lookup import FolderLookup


class DeleteFolderOperator(BaseOperator):
    def __init__(
        self,
        folder: Union[FolderLookup, str] = '',
        minio_conn_id: str = 'minio_default',
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)

        self.folder = folder
        self.minio_conn_id = minio_conn_id

    def execute(self, context: Context) -> None:
        if isinstance(self.folder, FolderLookup):
            self.folder = self.folder.get(context)

        minio = MinioHook(conn_id=self.minio_conn_id)

        minio.delete_folder(folder=self.folder)
