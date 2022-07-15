from dataclasses import dataclass
from typing import Any
from airflow.models.xcom_arg import XComArg


@dataclass(frozen=True)
class FolderLookup:
    raw: XComArg
    path: str = '{raw}'

    def resolve(self, context) -> Any:
        data = self.raw.resolve(context)
        return self.path.format(raw=data)
