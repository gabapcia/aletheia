import asyncio
from datetime import datetime, timedelta
import shutil
from contextlib import suppress
from pathlib import Path
from django.conf import settings
from django.db import IntegrityError
from celery import shared_task
from aletheia.scraper.downloader import DefaultDownloader
from aletheia.celery.task import RetryTask
from aletheia.utils.file import Chunkenizer
from .scraper.client import Client
from .engine.extractor import Extractor
from .engine.data_insertion import Engine
from .models import Release, InsertionTask


@shared_task(base=RetryTask)
def insert(task_id: str) -> None:
    task: InsertionTask = InsertionTask.objects.get(pk=task_id)
    release: Release = task.release

    engine = Engine(task)
    engine.insert()

    task.finished = True
    task.save()

    tasks_finished = release.insertiontask_set.all().values_list('finished', flat=True)
    if all(tasks_finished):
        release.finished = True
        release.save()

        with suppress(FileNotFoundError):
            shutil.rmtree(release.folder)


@shared_task(base=RetryTask)
def chunkenize(release_id: str, filepath: str, filetype: InsertionTask.Type) -> None:
    release: Release = Release.objects.get(pk=release_id)
    filepath = Path(filepath)

    chunkenizer = Chunkenizer(filepath)

    tasks = []
    for start, end in chunkenizer.chunkenize(n_chunks=100):
        task = InsertionTask(
            release=release,
            type=filetype,
            filepath=filepath,
            start=start,
            end=end,
        )
        tasks.append(task)

    InsertionTask.objects.bulk_create(tasks)
    for task in tasks:
        insert.s(task_id=task.pk).apply_async()


@shared_task(base=RetryTask)
def download(release_id: str) -> None:
    release: Release = Release.objects.get(pk=release_id)

    output_folder = Path(release.folder)

    downloader = DefaultDownloader(release.uri)
    zip_path = asyncio.run(downloader.download(output_folder))

    extractor = Extractor(zip_path)
    files = extractor.extract(output_folder)

    chunkenize.s(
        release_id=release.pk,
        filepath=files.trip_file.as_posix(),
        filetype=InsertionTask.Type.TRIP,
    ).apply_async()
    chunkenize.s(
        release_id=release.pk,
        filepath=files.trip_part_file.as_posix(),
        filetype=InsertionTask.Type.TRIP_PART,
    ).apply_async()
    chunkenize.s(
        release_id=release.pk,
        filepath=files.ticket_file.as_posix(),
        filetype=InsertionTask.Type.TICKET,
    ).apply_async()
    chunkenize.s(
        release_id=release.pk,
        filepath=files.payment_file.as_posix(),
        filetype=InsertionTask.Type.PAYMENT,
    ).apply_async()


@shared_task
def sync() -> None:
    responses = asyncio.run(Client().summary())

    current_year = datetime.now().year
    for r in responses:
        if r.year >= current_year:
            continue

        folder: Path = settings.DOWNLOAD_ROOT / f'CGUVIAGENS-{r.year}'

        try:
            release: Release = Release.objects.create(
                year=r.year,
                uri=r.uri,
                folder=folder.as_posix(),
            )
        except IntegrityError:
            continue

        folder.mkdir(exist_ok=True)

        download.s(release_id=release.pk).apply_async()


@shared_task
def healthcheck() -> None:
    deadline = datetime.now() - timedelta(days=2)
    pending_tasks = InsertionTask.objects.filter(finished=False, updated_at__lte=deadline)

    for task in pending_tasks:
        insert.s(task_id=task.pk).apply_async()
