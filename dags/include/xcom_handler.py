from typing import Any, List
from airflow import XComArg
from airflow.decorators import task


@task
def lookup_xcom(data: XComArg, lookup: List[Any]) -> Any:
    value = data

    for key in lookup:
        value = value[key]

    return value


@task(multiple_outputs=False)
def drop_null(data: List[Any]) -> List[Any]:
    return [d for d in data if d]
