from tempfile import NamedTemporaryFile
from zipfile import ZipFile
from typing import List, Union
from airflow import XComArg
from airflow.utils.context import Context
from airflow.models.baseoperator import BaseOperator
from minio_plugin.hooks.minio_hook import MinioHook
from minio_plugin.utils.lookup import FolderLookup
from minio_plugin.utils.wrapper import FileLike


class ExtractFileOperator(BaseOperator):
    def __init__(
        self,
        zip: Union[XComArg, str],
        bucket: str,
        folder: Union[FolderLookup, str] = '',
        filename: str = '',
        minio_conn_id: str = 'minio_default',
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)

        self.zip = zip
        self.bucket = bucket
        self.folder = folder
        self.filename = filename
        self.minio_conn_id = minio_conn_id

    def execute(self, context: Context) -> List[str]:
        if isinstance(self.zip, XComArg):
            self.zip = self.zip.resolve(context)

        if isinstance(self.folder, FolderLookup):
            self.folder = self.folder.resolve(context)

        minio = MinioHook(conn_id=self.minio_conn_id)
        with minio.get_object(bucket=self.bucket, name=self.zip) as f, NamedTemporaryFile(mode='wb') as tmp:
            while data := f.read(8 * 1024):
                tmp.write(data)

            tmp.flush()

            files = []
            with ZipFile(tmp.name, mode='r') as zip:
                for filename in zip.namelist():
                    with zip.open(filename, mode='r') as file:
                        path = minio.save(
                            reader=FileLike(file, name=self.filename),
                            bucket=self.bucket,
                            folder=self.folder,
                        )
                        files.append(path)

        return files
