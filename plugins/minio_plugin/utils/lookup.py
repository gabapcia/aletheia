from dataclasses import dataclass
from typing import Any, List
from airflow.models.xcom_arg import XComArg


@dataclass(frozen=True)
class XComArgLookup:
    raw: XComArg
    lookup: List[Any]

    def get(self, context) -> Any:
        data = self.raw.resolve(context)
        for l in self.lookup:
            data = data[l]

        return data


@dataclass(frozen=True)
class FolderLookup(XComArgLookup):
    path: str = '{raw}'

    def get(self, context) -> Any:
        raw = super().get(context)
        return self.path.format(raw=raw)
