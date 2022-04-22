from typing import ContextManager
from abc import ABC, abstractmethod
from contextlib import contextmanager
from urllib3.response import HTTPResponse
from airflow.hooks.base import BaseHook
from minio import Minio
from minio.helpers import ObjectWriteResult
from minio_plugin.http.file import HTTPFile


class FileReader(ABC):
    @abstractmethod
    def read(self, size: int) -> None:
        raise NotImplementedError

    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def size(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def content_type(self) -> str:
        raise NotImplementedError


class MinioHook(BaseHook):
    conn_type = 'minio'
    hook_name = 'MinIO'

    def __init__(self, conn_id: str = 'minio_default', *args, **kwargs):
        super().__init__(*args, **kwargs)

        conn = self.get_connection(conn_id)

        self.bucket = conn.extra_dejson['bucket']
        self.client = Minio(
            endpoint=f'{conn.host}:{conn.port}',
            access_key=conn.login,
            secret_key=conn.password,
            secure=conn.extra_dejson.get('secure', False),
        )

    def save_http(self, file: HTTPFile, folder: str = '') -> str:
        if folder and not folder.endswith('/'):
            folder += '/'

        r: ObjectWriteResult = self.client.put_object(
            bucket_name=self.bucket,
            object_name=f'{folder}{file.name()}',
            data=file,
            length=file.size(),
            content_type=file.content_type(),
            part_size=5 * 1024 * 1024,
            metadata=None,
            sse=None,
            tags=None,
            retention=None,
        )

        return r.object_name

    def save(self, reader: FileReader, folder: str = '') -> str:
        if folder and not folder.endswith('/'):
            folder += '/'

        r: ObjectWriteResult = self.client.put_object(
            bucket_name=self.bucket,
            object_name=f'{folder}{reader.name()}',
            data=reader,
            length=reader.size(),
            content_type=reader.content_type(),
            part_size=5 * 1024 * 1024,
            metadata=None,
            sse=None,
            tags=None,
            retention=None,
        )

        return r.object_name

    def delete_folder(self, folder: str) -> None:
        objects = self.client.list_objects(bucket_name=self.bucket, prefix=folder, recursive=True)

        for obj in objects:
            self.client.remove_object(bucket_name=self.bucket, object_name=obj.object_name)

    @contextmanager
    def get(self, name: str, *args, **kwargs) -> ContextManager[HTTPResponse]:
        r: HTTPResponse = self.client.get_object(bucket_name=self.bucket, object_name=name, *args, **kwargs)

        try:
            yield r
        finally:
            r.close()
            r.release_conn()
