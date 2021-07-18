from dataclasses import dataclass
from datetime import date


@dataclass
class FileResponse:
    date: date
    name: str
    uri: str
    size: str


@dataclass
class Response:
    reference_date: date
    employee: FileResponse
    retired: FileResponse
    pensioner: FileResponse
