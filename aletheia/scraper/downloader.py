import re
from pathlib import Path
from typing import Any
import httpx
from aiofile import async_open


class DefaultDownloader:
    def __init__(self, uri: str) -> None:
        self.uri = uri
        self._default_filename = uri.split('/')[-1]
        self._zip_path = None
        self._file_headers = None

    async def download(self, output_folder: Path) -> Path:
        filename = await self._get_file_name()
        self._zip_path = output_folder / filename
        self._zip_path.touch(exist_ok=True)

        total_size = await self._get_file_size()
        current_size = self._zip_path.stat().st_size

        if current_size >= total_size:
            return self._zip_path

        headers = {'Range': f'bytes={current_size}-'}
        async with httpx.AsyncClient(timeout=20) as client, client.stream('GET', self.uri, headers=headers) as r:
            r.raise_for_status()

            async with async_open(self._zip_path, mode='ab') as f:
                async for chunck in r.aiter_bytes(chunk_size=8 * 1024):
                    await f.write(chunck)

        return self._zip_path

    async def _get_headers(self) -> dict[str, Any]:
        if not self._file_headers:
            async with httpx.AsyncClient(timeout=20) as client:
                r = await client.head(self.uri)

                self._file_headers = r.headers

        return self._file_headers

    async def _get_file_size(self) -> int:
        headers = await self._get_headers()
        size = int(headers['Content-Length'])
        return size

    async def _get_file_name(self) -> str:
        headers = await self._get_headers()
        re_filename = re.search(r'filename\=\"(?P<filename>[^\"]+)\"', headers['Content-Disposition'])
        if re_filename:
            filename = re_filename.group('filename')
        else:
            filename = self._default_filename

        return filename
