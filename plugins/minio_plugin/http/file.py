import httpx


class HTTPFile:
    def __init__(self, uri: str, name: str = '', timeout: int = 5) -> None:
        self._uri = uri
        self._name = name
        self._timeout = timeout

    def __enter__(self) -> 'HTTPFile':
        self._stream = httpx.stream('GET', self._uri, timeout=self._timeout)
        self._response = self._stream.__enter__()
        self._response.raise_for_status()

        self._data = self._response.iter_bytes()
        return self

    def __exit__(self, *args, **kwargs) -> None:
        self._response.close()
        self._stream.__exit__(*args, **kwargs)

    def read(self, size: int) -> bytes:
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
        cl = int(self._response.headers.get('Content-Length', -1))
        return cl

    def name(self) -> str:
        name = self._name or self._uri.split('/')[-1].split('?')[0]
        return name
