import asyncio
import shutil
from contextlib import suppress
from datetime import datetime, timedelta
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
    if not task.finished:
        engine = Engine(task)
        engine.insert()

        task.finished = True
        task.save()

    if not release.finished:
        tasks_finished = release.insertiontask_set.all().values_list('finished', flat=True)
        if all(tasks_finished):
            release.finished = True
            release.save()

            with suppress(FileNotFoundError):
                shutil.rmtree(release.folder)


@shared_task(base=RetryTask)
def chunkenize(release_id: str, payment_filepath: str, withdraw_filepath: str) -> None:
    release: Release = Release.objects.get(pk=release_id)

    payment_filepath = Path(payment_filepath)
    withdraw_filepath = Path(withdraw_filepath)

    tasks = []

    chunkenizer = Chunkenizer(payment_filepath)
    for start, end in chunkenizer.chunkenize():
        task = InsertionTask(
            release=release,
            type=InsertionTask.Type.PAYMENT,
            filepath=payment_filepath,
            start=start,
            end=end,
        )
        tasks.append(task)

    chunkenizer = Chunkenizer(withdraw_filepath)
    for start, end in chunkenizer.chunkenize():
        task = InsertionTask(
            release=release,
            type=InsertionTask.Type.WITHDRAW,
            filepath=withdraw_filepath,
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

    async def download_all() -> tuple[Path, Path]:
        payment_downloader = DefaultDownloader(release.payment_uri)
        withdraw_downloader = DefaultDownloader(release.withdraw_uri)

        payment_zip = payment_downloader.download(output_folder)
        withdraw_zip = withdraw_downloader.download(output_folder)

        return await asyncio.gather(payment_zip, withdraw_zip)

    payment_zip, withdraw_zip = asyncio.run(download_all())

    payment_extractor = DefaultExtractor(payment_zip)
    withdraw_extractor = DefaultExtractor(withdraw_zip)
    payment_filepath = payment_extractor.extract(output_folder)[0]
    withdraw_filepath = withdraw_extractor.extract(output_folder)[0]

    chunkenize.s(
        release_id=release.pk,
        payment_filepath=payment_filepath.as_posix(),
        withdraw_filepath=withdraw_filepath.as_posix(),
    ).apply_async()


@shared_task
def sync() -> None:
    responses = asyncio.run(Client().summary())

    for r in responses:
        folder: Path = settings.DOWNLOAD_ROOT / f'CGUBOLSAFAMILIA-{r.generated_at.isoformat()}'

        try:
            release: Release = Release.objects.create(
                date=r.generated_at,
                folder=folder.as_posix(),
                payment_uri=r.payment_uri,
                withdraw_uri=r.withdraw_uri,
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
