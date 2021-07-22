import asyncio
from datetime import datetime, timedelta
import shutil
from contextlib import suppress
from pathlib import Path
from django.conf import settings
from django.db import IntegrityError
from celery import shared_task
from aletheia.scraper.downloader import DefaultDownloader
from aletheia.scraper.extractor import DefaultExtractor
from aletheia.celery.task import RetryTask
from aletheia.utils.file import Chunkenizer
from .scraper.client import Client
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
def chunkenize(release_id: str, filepath: str) -> None:
    release: Release = Release.objects.get(pk=release_id)
    filepath = Path(filepath)

    chunkenizer = Chunkenizer(filepath)

    tasks = []
    for start, end in chunkenizer.chunkenize(n_chunks=100):
        task = InsertionTask(
            release=release,
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

    extractor = DefaultExtractor(zip_path)
    filepath = extractor.extract(output_folder)[0]

    chunkenize.s(release_id=release.pk, filepath=filepath.as_posix()).apply_async()


@shared_task
def sync() -> None:
    responses = asyncio.run(Client().summary())

    for r in responses:
        folder: Path = settings.DOWNLOAD_ROOT / f'CGUGARANTIASAFRA-{r.reference_date.isoformat()}'

        try:
            release: Release = Release.objects.create(
                date=r.reference_date,
                folder=folder.as_posix(),
                uri=r.uri,
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
