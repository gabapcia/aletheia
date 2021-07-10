import asyncio
from enum import IntEnum
from datetime import datetime, timedelta
from pathlib import Path
from celery import shared_task
from django.conf import settings
from django.db import IntegrityError
from aletheia.celery.task import RetryTask
from .scraper.client import Client
from .scraper.downloader import Downloader
from .scraper.extractor import Extractor
from .models import Release, InsertionTask
from .engine.data_insertion import Engine


class FileType(IntEnum):
    COMPANY = 0
    SIMPLES_MEI = 1
    ESTABLISHMENT = 2
    PARTNER = 3


@shared_task(base=RetryTask)
def insert(task_id: str) -> None:
    task: InsertionTask = InsertionTask.objects.get(pk=task_id)
    release: Release = task.release
    if not task.finished:
        engine = Engine(task)
        engine.insert()

        task.finished = True
        task.save()

    tasks_finished = InsertionTask.objects.filter(release=release.pk).values_list('finished', flat=True)
    if all(tasks_finished):
        release.finished = True
        release.save()


@shared_task(base=RetryTask)
def chunkenize(release_id: str, filepath: str, filetype: FileType) -> None:
    release = Release.objects.get(pk=release_id)

    CHUNKS = 100
    tasks_type = {
        FileType.PARTNER: InsertionTask.Type.PARTNERS,
        FileType.COMPANY: InsertionTask.Type.COMPANIES,
        FileType.ESTABLISHMENT: InsertionTask.Type.ESTABLISHMENTS,
        FileType.SIMPLES_MEI: InsertionTask.Type.SIMPLES,
    }[filetype]

    with open(filepath, 'r', encoding='iso-8859-1') as f:
        lines = sum(1 for _ in f)

    chunk_size = lines // CHUNKS
    tasks = []
    for i in range(CHUNKS):
        start = i * chunk_size
        end = (i + 1) * chunk_size - 1

        if i == (CHUNKS - 1):
            end = lines

        task = InsertionTask(
            release=release,
            type=tasks_type,
            filepath=filepath,
            start=start,
            end=end,
        )
        tasks.append(task)

    InsertionTask.objects.bulk_create(tasks)
    for task in tasks:
        insert.s(task_id=task.pk).apply_async()


@shared_task(base=RetryTask)
def download(release_id: str, uri: str, filetype: FileType) -> None:
    release = Release.objects.get(pk=release_id)
    downloader = Downloader(uri)
    zip_path = asyncio.run(downloader.download(Path(release.folder)))

    extractor = Extractor(zip_path)
    filepath = extractor.extract(zip_path.parent)[0]

    chunkenize.s(release_id=release_id, filepath=filepath.as_posix(), filetype=filetype).apply_async()


@shared_task
def sync() -> None:
    rfb_client = Client()
    r = asyncio.run(rfb_client.summary())

    folder: Path = settings.DOWNLOAD_ROOT / r.generated_at.isoformat()

    try:
        release = Release.objects.create(release_date=r.generated_at, folder=folder.as_posix())
    except IntegrityError:
        return

    folder.mkdir(exist_ok=True)

    for uri in r.company_uris:
        download.s(release_id=release.pk, uri=uri, filetype=FileType.COMPANY).apply_async()

    download.s(release_id=release.pk, uri=r.simples_mei_info, filetype=FileType.SIMPLES_MEI).apply_async()

    for uri in r.company_place_uris:
        download.s(release_id=release.pk, uri=uri, filetype=FileType.ESTABLISHMENT).apply_async()

    for uri in r.partner_uris:
        download.s(release_id=release.pk, uri=uri, filetype=FileType.PARTNER).apply_async()


@shared_task
def healthcheck() -> None:
    release = Release.objects.filter(finished=False).first()
    if not release:
        return

    deadline = datetime.now() - timedelta(days=2)
    tasks = release.insertiontask_set.filter(finished=False, updated_at__lte=deadline)
    for task in tasks:
        insert.s(task_id=task.pk).apply_async()
        task.save()  # Update field "updated_at"
