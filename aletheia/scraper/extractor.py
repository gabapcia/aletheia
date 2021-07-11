from pathlib import Path
from zipfile import ZipFile


class DefaultExtractor:
    def __init__(self, zip_path: Path) -> None:
        self.zip = zip_path
        self.files = []

    def extract(self, path: Path) -> list[Path]:
        with ZipFile(self.zip, mode='r') as f:
            files = [path / name for name in f.namelist()]
            f.extractall(path=path)

        self.files = files
        return files
