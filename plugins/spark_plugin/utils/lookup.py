from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Union
from airflow import XComArg
from airflow.utils.context import Context
from airflow.hooks.base import BaseHook


@dataclass(frozen=True)
class ConfFromConnection:
    conn_id: str
    field: Union[List[str], str]
    format: Optional[str] = '{}'
    callback: Callable[[str], str] = None

    def get(self) -> str:
        conn = BaseHook.get_connection(self.conn_id)

        if not isinstance(self.field, list):
            value = getattr(conn, self.field)

            if self.callback:
                value = self.callback(value)

            return self.format.format(value)

        fields = {}
        for f in self.field:
            fields[f] = getattr(conn, f)

            if self.callback:
                fields[f] = self.callback(fields[f])

        return self.format.format(**fields)


@dataclass(frozen=True)
class ConfFromXCom:
    data: XComArg
    lookup: List[Any]

    def resolve(self, context: Context) -> str:
        data = self.data.resolve(context)

        for key in self.lookup:
            data = data[key]

        return data
