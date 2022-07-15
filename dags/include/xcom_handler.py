from typing import Any, List
from airflow import XComArg
from airflow.decorators import task


@task
def lookup_xcom(data: XComArg, lookup: List[Any]) -> Any:
    value = data

    for key in lookup:
        value = value[key]

    return value
