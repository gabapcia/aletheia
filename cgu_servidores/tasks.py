import asyncio
from datetime import datetime, timedelta
import logging
import shutil
from contextlib import suppress
from pathlib import Path
from django.conf import settings
from celery import shared_task
from django.db import IntegrityError
from aletheia.scraper.downloader import DefaultDownloader
from aletheia.celery.task import RetryTask
from .scraper.client import Client
from .engine.insertion.engine import Engine as InsertionEngine
from .engine.extractor import EmployeeExtractor, RetiredExtractor, PensionerExtractor
from .engine.validator import EmployeeValidator, RetiredValidator, PensionerValidator
from .engine.validator.exceptions import BaseValidatorException
from .models import Release, InsertionTask


@shared_task(base=RetryTask)
def insert(task_id: str) -> None:
    for release in Release.objects.all():
        assert release.finished or release.valid, 'Tasks not ready to be inserted'

    task: InsertionTask = InsertionTask.objects.get(pk=task_id)
    release: Release = task.release

    employee_types = [
        InsertionTask.Type.EMPLOYEE,
        InsertionTask.Type.RETIRED,
        InsertionTask.Type.PENSIONER,
    ]
    valid = True
    if task.type in employee_types:
        newest_release = Release.objects.filter(valid=True).order_by('-date').first()
        if str(release.pk) != str(newest_release.pk):
            valid = False
            task.finished = True
            task.save()

    if valid and not task.finished:
        engine = InsertionEngine(task)
        engine.insert()

        task.finished = True
        task.save()

    tasks_finished = release.insertiontask_set.all().values_list('finished', flat=True)
    if all(tasks_finished):
        folder = Path(release.folder)

        release.finished = True
        release.save()

        with suppress(FileNotFoundError):
            shutil.rmtree(folder)


@shared_task(base=RetryTask)
def download(release_id: str) -> None:
    release: Release = Release.objects.get(pk=release_id)
    folder = Path(release.folder)

    async def download_all() -> tuple[Path, Path, Path]:
        employee = DefaultDownloader(release.employee_uri)
        retired = DefaultDownloader(release.retired_uri)
        pensioner = DefaultDownloader(release.pensioner_uri)

        return await asyncio.gather(
            employee.download(folder),
            retired.download(folder),
            pensioner.download(folder),
        )

    employee_zip, retired_zip, pensioner_zip = asyncio.run(download_all())

    try:
        EmployeeValidator(employee_zip).validate()
        RetiredValidator(retired_zip).validate()
        PensionerValidator(pensioner_zip).validate()
    except BaseValidatorException as e:
        release.finished = True
        release.valid = False
        release.save()

        with suppress(FileNotFoundError):
            shutil.rmtree(Path(release.folder))

        logging.warning(f'Invalid Release: [{release.date}] - {e}')
        return

    release.valid = True
    release.save()

    employee_folder = folder / 'employee'
    employee_folder.mkdir(exist_ok=True)

    retired_folder = folder / 'retired'
    retired_folder.mkdir(exist_ok=True)

    pensioner_folder = folder / 'pensioner'
    pensioner_folder.mkdir(exist_ok=True)

    employee_data = EmployeeExtractor(employee_zip, employee_folder).extract()
    retired_data = RetiredExtractor(retired_zip, retired_folder).extract()
    pensioner_data = PensionerExtractor(pensioner_zip, pensioner_folder).extract()

    tasks = [
        InsertionTask(file=employee_data.honorarios.advocaticios.as_posix(), type=InsertionTask.Type.FEE, release=release),
        InsertionTask(file=employee_data.honorarios.jetons.as_posix(), type=InsertionTask.Type.FEE, release=release),
        InsertionTask(file=employee_data.militares.cadastro.as_posix(), type=InsertionTask.Type.EMPLOYEE, release=release),
        InsertionTask(file=employee_data.militares.observacoes.as_posix(), type=InsertionTask.Type.OBSERVATION, release=release),
        InsertionTask(file=employee_data.militares.remuneracao.as_posix(), type=InsertionTask.Type.REMUNERATION, release=release),
        InsertionTask(file=employee_data.servidores_bacen.cadastro.as_posix(), type=InsertionTask.Type.EMPLOYEE, release=release),
        InsertionTask(file=employee_data.servidores_bacen.remuneracao.as_posix(), type=InsertionTask.Type.REMUNERATION, release=release),
        InsertionTask(file=employee_data.servidores_bacen.afastamentos.as_posix(), type=InsertionTask.Type.TIMEOFF, release=release),
        InsertionTask(file=employee_data.servidores_bacen.remuneracao.as_posix(), type=InsertionTask.Type.REMUNERATION, release=release),
        InsertionTask(file=employee_data.servidores_siape.cadastro.as_posix(), type=InsertionTask.Type.EMPLOYEE, release=release),
        InsertionTask(file=employee_data.servidores_siape.afastamentos.as_posix(), type=InsertionTask.Type.TIMEOFF, release=release),
        InsertionTask(file=employee_data.servidores_siape.observacoes.as_posix(), type=InsertionTask.Type.OBSERVATION, release=release),
        InsertionTask(file=employee_data.servidores_siape.remuneracao.as_posix(), type=InsertionTask.Type.REMUNERATION, release=release),
        InsertionTask(file=retired_data.bacen.cadastro.as_posix(), type=InsertionTask.Type.RETIRED, release=release),
        InsertionTask(file=retired_data.bacen.remuneracao.as_posix(), type=InsertionTask.Type.REMUNERATION, release=release),
        InsertionTask(file=retired_data.bacen.observacoes.as_posix(), type=InsertionTask.Type.OBSERVATION, release=release),
        InsertionTask(file=retired_data.siape.cadastro.as_posix(), type=InsertionTask.Type.RETIRED, release=release),
        InsertionTask(file=retired_data.siape.observacoes.as_posix(), type=InsertionTask.Type.OBSERVATION, release=release),
        InsertionTask(file=retired_data.siape.remuneracao.as_posix(), type=InsertionTask.Type.REMUNERATION, release=release),
        InsertionTask(file=retired_data.militar.cadastro.as_posix(), type=InsertionTask.Type.RETIRED, release=release),
        InsertionTask(file=retired_data.militar.observacoes.as_posix(), type=InsertionTask.Type.OBSERVATION, release=release),
        InsertionTask(file=retired_data.militar.remuneracao.as_posix(), type=InsertionTask.Type.REMUNERATION, release=release),
        InsertionTask(file=pensioner_data.bacen.cadastro.as_posix(), type=InsertionTask.Type.PENSIONER, release=release),
        InsertionTask(file=pensioner_data.bacen.remuneracao.as_posix(), type=InsertionTask.Type.REMUNERATION, release=release),
        InsertionTask(file=pensioner_data.bacen.observacoes.as_posix(), type=InsertionTask.Type.OBSERVATION, release=release),
        InsertionTask(file=pensioner_data.siape.cadastro.as_posix(), type=InsertionTask.Type.PENSIONER, release=release),
        InsertionTask(file=pensioner_data.siape.observacoes.as_posix(), type=InsertionTask.Type.OBSERVATION, release=release),
        InsertionTask(file=pensioner_data.siape.remuneracao.as_posix(), type=InsertionTask.Type.REMUNERATION, release=release),
        InsertionTask(file=pensioner_data.militar.cadastro.as_posix(), type=InsertionTask.Type.PENSIONER, release=release),
        InsertionTask(file=pensioner_data.militar.observacoes.as_posix(), type=InsertionTask.Type.OBSERVATION, release=release),
        InsertionTask(file=pensioner_data.militar.remuneracao.as_posix(), type=InsertionTask.Type.REMUNERATION, release=release),
    ]

    InsertionTask.objects.bulk_create(tasks)
    for task in tasks:
        insert.s(task_id=task.id).apply_async()


@shared_task
def sync() -> None:
    responses = asyncio.run(Client().summary())

    for r in responses:
        folder: Path = settings.DOWNLOAD_ROOT / f'CGUSERVIDORES-{r.reference_date.isoformat()}'
        try:
            release: Release = Release.objects.create(
                folder=folder.as_posix(),
                date=r.reference_date,
                employee_uri=r.employee.uri,
                retired_uri=r.retired.uri,
                pensioner_uri=r.pensioner.uri,
            )
        except IntegrityError:
            continue
        else:
            folder.mkdir(exist_ok=True)

        download.s(release_id=release.pk).apply_async()


@shared_task
def healthcheck() -> None:
    deadline = datetime.utcnow() - timedelta(days=2)

    releases = Release.objects.filter(insertiontask__isnull=True, finished=False, updated_at__lte=deadline)
    for release in releases:
        download.s(release_id=release.pk).apply_async()

    tasks = InsertionTask.objects.filter(finished=False, updated_at__lte=deadline)
    for task in tasks:
        insert.s(task_id=task.pk).apply_async()

    deadline = datetime.utcnow() - timedelta(days=30)

    Release.objects.filter(finished=True, valid=False).delete()
