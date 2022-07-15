from typing import ContextManager
from contextlib import contextmanager
from urllib3.response import HTTPResponse
from airflow.hooks.base import BaseHook
from minio import Minio
from minio.helpers import ObjectWriteResult
from minio_plugin.utils.file import FileReader


class MinioHook(BaseHook):
    conn_type = 'minio'
    hook_name = 'MinIO'

    def __init__(self, conn_id: str = 'minio_default', *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        conn = self.get_connection(conn_id)

        self.client = Minio(
            endpoint=f'{conn.host}:{conn.port}',
            access_key=conn.login,
            secret_key=conn.password,
            secure=conn.extra_dejson.get('secure', False),
        )

    def get_client(self) -> Minio:
        return self.client

    def save(self, reader: FileReader, bucket: str, folder: str = '') -> str:
        if folder.endswith('/'):
            folder = folder[:-1]

        key = f'{folder}/{reader.name()}'

        r: ObjectWriteResult = self.client.put_object(
            bucket_name=bucket,
            object_name=key,
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

    def delete_folder(self, folder: str, bucket: str) -> None:
        objects = self.client.list_objects(bucket_name=bucket, prefix=folder, recursive=True)

        for obj in objects:
            self.client.remove_object(bucket_name=bucket, object_name=obj.object_name)

    @contextmanager
    def get_object(self, name: str, bucket: str, *args, **kwargs) -> ContextManager[HTTPResponse]:
        r: HTTPResponse = self.client.get_object(bucket_name=bucket, object_name=name, *args, **kwargs)

        try:
            yield r
        finally:
            r.close()
            r.release_conn()
