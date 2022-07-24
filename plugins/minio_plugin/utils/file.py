from abc import ABC, abstractmethod
import re
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
    def __init__(self, uri: str, name: str = '', timeout: int = 5, **attrs) -> None:
        self._uri = uri
        self._name = name
        self._timeout = timeout
        self._attrs = attrs

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
        ct = self._attrs.get('content_type', None)
        if ct:
            return ct

        ct = self._response.headers['Content-Type']
        return ct

    def size(self) -> int:
        cl = self._attrs.get('size', None)
        if cl:
            return cl

        cl = int(self._response.headers.get('Content-Length', -1))
        return cl

    def name(self) -> str:
        filename_re = re.search(r'filename="(?P<data>[^"])"', self._response.headers.get('Content-Disposition', ''))

        if filename_re:
            name = filename_re.group('data')
            return name

        name = self._name or self._uri.split('/')[-1].split('?')[0]
        return name
