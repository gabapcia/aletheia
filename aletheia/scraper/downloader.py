from pathlib import Path
import httpx
from aiofile import async_open


class DefaultDownloader:
    def __init__(self, uri: str) -> None:
        self.uri = uri
        self.filename = uri.split('/')[-1]
        self._zip_path = None

    async def download(self, output_folder: Path) -> Path:
        self._zip_path = output_folder / self.filename
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

    async def _get_file_size(self) -> int:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.head(self.uri)
            size = int(r.headers['Content-Length'])

        return size
