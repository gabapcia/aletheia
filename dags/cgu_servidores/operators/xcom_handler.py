from typing import Any, Dict, List, Tuple
from airflow.decorators import task
from cgu_servidores.operators.scraper import FILEDATE_KEY


FILENAME_KEY = 'filename'
FILE_URI_KEY = 'uri'
FILE_SUBTYPE_KEY = 'type'


@task(multiple_outputs=False)
def get_files_by_type(all_links: List[Dict[str, Any]], file_type: str) -> List[Tuple[str, Tuple[str, str]]]:
    data = list()

    for group_links in all_links:
        for filename, uri in group_links[file_type].items():
            data.append({
                FILEDATE_KEY: group_links[FILEDATE_KEY],
                FILENAME_KEY: filename,
                FILE_URI_KEY: uri,
                FILE_SUBTYPE_KEY: filename.split('_')[-1].lower(),
            })

    return data
