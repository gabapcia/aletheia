from dataclasses import dataclass
from typing import List, Optional, Union
from airflow.hooks.base import BaseHook


@dataclass(frozen=True)
class ConfFromConnection:
    conn_id: str
    field: Union[List[str], str]
    format: Optional[str] = '{}'

    def get(self) -> str:
        conn = BaseHook.get_connection(self.conn_id)

        if not isinstance(self.field, list):
            return self.format.format(getattr(conn, self.field))

        fields = {f: getattr(conn, f) for f in self.field}
        return self.format.format(**fields)
