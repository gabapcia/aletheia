from datetime import date
from typing import Any, Dict
from airflow.decorators import task
from minio_plugin.hooks.minio_hook import MinioHook
from tse_candidatos.operators.file_storage import MINIO_CONN_ID, MINIO_BUCKET, FILES_DATE_KEY


@task(multiple_outputs=False)
def idempotence(catalog: Dict[int, Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    hook = MinioHook(conn_id=MINIO_CONN_ID)
    minio = hook.get_client()

    catalog_cleaned = dict()

    for year, files in catalog.items():
        max_date = max(map(lambda f: date.fromisoformat(f['updated_at']), files.values()))

        filepath_prefix = f'/{year}/{max_date.isoformat()}'

        objects = list(minio.list_objects(MINIO_BUCKET, prefix=filepath_prefix, recursive=False))
        if len(objects) > 0:
            continue

        files[FILES_DATE_KEY] = max_date.isoformat()
        catalog_cleaned[year] = files

    return catalog_cleaned
