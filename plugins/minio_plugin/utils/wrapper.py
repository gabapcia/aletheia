from typing import IO
import magic
from unidecode import unidecode


class FileLike:
    def __init__(self, reader: IO[bytes], name: str = '') -> None:
        self._name = name

        previous_position = reader.tell()
        self._content_type = magic.from_buffer(reader.read(1024), mime=True)

        reader.seek(previous_position)
        self.reader = reader

    def read(self, size: int) -> None:
        return self.reader.read(size)

    def name(self) -> str:
        name = self._name or self.reader.name
        name = '_'.join(unidecode(name).lower().split())
        return name

    def size(self) -> int:
        return -1

    def content_type(self) -> str:
        return self._content_type
