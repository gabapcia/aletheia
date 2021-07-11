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
def clean(release_id: str) -> None:
    release: Release = Release.objects.get(pk=release_id)

    with suppress(FileNotFoundError):
        shutil.rmtree(release.folder)

    Release.objects.exclude(pk=release_id).delete()


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

        clean.s(release_id=release.pk).apply_async()


@shared_task(base=RetryTask)
def chunckenize(release_id: str, filepath: str) -> None:
    release: Release = Release.objects.get(pk=release_id)
    filepath = Path(filepath)

    chunckenizer = Chunkenizer(filepath)

    tasks = []
    for start, end in chunckenizer.chunckenize(n_chunks=100):
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
def download(release_id: str, uri: str) -> None:
    release: Release = Release.objects.get(pk=release_id)

    output_folder = Path(release.folder)

    downloader = DefaultDownloader(uri)
    zip_path = downloader.download(output_folder)

    extractor = DefaultExtractor(zip_path)
    filepath = extractor.extract(output_folder)


@shared_task
def sync() -> None:
    r = asyncio.run(Client().summary())

    folder: Path = settings.DOWNLOAD_ROOT / f'GCUPEP-{r.generated_at.isoformat()}'

    try:
        release = Release.objects.create(date=r.generated_at, folder=folder.as_posix())
    except IntegrityError:
        return

    folder.mkdir(exist_ok=True)

    download.s(release_id=release.pk, uri=r.uri).apply_async()


@shared_task
def healthcheck() -> None:
    deadline = datetime.now() - timedelta(days=2)
    pending_tasks = InsertionTask.objects.filter(finished=False, updated_at__lte=deadline)

    for task in pending_tasks:
        insert.s(task_id=task.pk).apply_async()
