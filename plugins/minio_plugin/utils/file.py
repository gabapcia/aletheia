from abc import ABC, abstractmethod
import httpx
from httpx import codes


class FileNotFound(Exception):
    pass


class FileReader(ABC):
    @abstractmethod
    def read(self, size: int) -> None:
        raise NotImplementedError

    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def size(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def content_type(self) -> str:
        raise NotImplementedError


class HTTPFile(FileReader):
    def __init__(self, uri: str, name: str = '', timeout: int = 5) -> None:
        self._uri = uri
        self._name = name
        self._timeout = timeout

    def __enter__(self) -> 'HTTPFile':
        self._stream = httpx.stream('GET', self._uri, timeout=self._timeout)
        self._response = self._stream.__enter__()

        if self._response.status_code == codes.NOT_FOUND:
            raise FileNotFound

        self._response.raise_for_status()

        self._data = None
        return self

    def __exit__(self, *args, **kwargs) -> None:
        self._response.close()
        self._stream.__exit__(*args, **kwargs)

    def read(self, size: int) -> bytes:
        if not self._data:
            self._data = self._response.iter_bytes(chunk_size=size)

        buffer = bytes()

        for data in self._data:
            buffer += data
            if len(buffer) >= size:
                break

        return buffer

    def content_type(self) -> str:
        ct = self._response.headers['Content-Type']
        return ct

    def size(self) -> int:
        co = int(self._response.headers.get('Content-Length', -1))
        return co

    def name(self) -> str:
        name = self._name or self._uri.split('/')[-1].split('?')[0]
        return name
