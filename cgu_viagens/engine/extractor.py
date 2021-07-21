from dataclasses import dataclass
from pathlib import Path
from zipfile import ZipFile


@dataclass
class Files:
    trip_file: Path
    trip_part_file: Path
    ticket_file: Path
    payment_file: Path


class Extractor:
    def __init__(self, zip_path: Path) -> None:
        self.zip = zip_path
        self.files = None

    def extract(self, path: Path) -> Files:
        with ZipFile(self.zip, mode='r') as f:
            files = [path / name for name in f.namelist()]
            f.extractall(path=path)

        self.files = Files(
            trip_file=self._get_trips_file(files),
            trip_part_file=self._get_trip_parts_file(files),
            ticket_file=self._get_tickets_file(files),
            payment_file=self._get_payments_file(files),
        )
        return self.files

    def _get_trips_file(self, files: list[Path]) -> Path:
        for file in files:
            if file.name.endswith('_Viagem.csv'):
                return file

    def _get_trip_parts_file(self, files: list[Path]) -> Path:
        for file in files:
            if file.name.endswith('_Trecho.csv'):
                return file

    def _get_tickets_file(self, files: list[Path]) -> Path:
        for file in files:
            if file.name.endswith('_Passagem.csv'):
                return file

    def _get_payments_file(self, files: list[Path]) -> Path:
        for file in files:
            if file.name.endswith('_Pagamento.csv'):
                return file
