import asyncio
import json
import logging
import re
from contextlib import asynccontextmanager
from datetime import datetime
from collections.abc import AsyncIterator
import httpx
from bs4 import BeautifulSoup
from unidecode import unidecode
from .response import FileResponse, Response


class Client:
    BASE_URL = 'http://www.portaldatransparencia.gov.br'
    INITIAL_URI = BASE_URL + '/download-de-dados/servidores'

    async def summary(self) -> list[Response]:
        async with httpx.AsyncClient() as client:
            r = await client.get(Client.INITIAL_URI)

        soup = BeautifulSoup(r.content, 'html.parser')

        for a in soup.select('p > a'):
            if 'servidores' == unidecode(a.text.strip()).lower():
                employee_uri = a['href']
            elif 'aposentados' == unidecode(a.text.strip()).lower():
                retired_uri = a['href']
            elif 'pensionistas' == unidecode(a.text.strip()).lower():
                pensioner_uri = a['href']

        response = await self._join_responses(employee_uri, retired_uri, pensioner_uri)
        return response

    async def _join_responses(self, employee_uri: str, retired_uri: str, pensioner_uri: str) -> list[Response]:
        e_files, r_files, p_files = await asyncio.gather(
            self._get_files_data(employee_uri),
            self._get_files_data(retired_uri),
            self._get_files_data(pensioner_uri),
        )

        responses = []
        for i in range(min(len(e_files), len(r_files), len(p_files))):
            if not (e_files[i].date == r_files[i].date == p_files[i].date):
                missing_date = min(e_files[i].date, r_files[i].date, p_files[i].date)
                logging.warning(f'Missing file for date {missing_date.isoformat()}...')
                return responses

            r = Response(
                reference_date=e_files[i].date,
                employee=e_files[i],
                retired=r_files[i],
                pensioner=p_files[i],
            )
            responses.append(r)

        return responses

    async def _get_files_data(self, uri: str) -> list[FileResponse]:
        async with self._get_client(http2=True, timeout=20) as client:
            r = await client.get(uri)

            soup = BeautifulSoup(r.content, 'html.parser')

            for script in soup.find_all('script'):
                if match := re.search(r'; var g_listData = (?P<data>[\s\S]+);if', str(script)):
                    break

            data = json.loads(match.group('data'))['ListData']['Row']
            files_data = []
            for d in data:
                details = self._get_file_data(d['.spItemUrl'], cookies=r.cookies)
                files_data.append(details)

            files_data = await asyncio.gather(*files_data)

        files_data = sorted(files_data, key=lambda f: f.date)
        return files_data

    async def _get_file_data(self, uri: str, **kwargs) -> FileResponse:
        async with self._get_client(http2=True, timeout=20) as client:
            r = await client.get(uri, **kwargs)

        data = r.json()
        file = FileResponse(
            date=datetime.strptime(data['name'].split('_')[0], '%Y%m').date(),
            name=data['name'],
            uri=data['@content.downloadUrl'],
            size=data['size'],
        )
        return file

    @asynccontextmanager
    async def _get_client(self, **kwargs) -> AsyncIterator[httpx.AsyncClient]:
        if 'headers' not in kwargs:
            default_headers = {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'
            }
            kwargs['headers'] = default_headers

        client = httpx.AsyncClient(**kwargs)
        try:
            yield client
        finally:
            await client.aclose()
