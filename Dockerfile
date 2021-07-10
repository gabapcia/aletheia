FROM python:3.9-slim as base

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y --no-install-recommends g++ netcat

WORKDIR /opt/app

ENV PYTHONUNBUFFERED 1

RUN pip install -U pip
COPY ./requirements.txt .
RUN pip install -Ur requirements.txt

COPY . .

RUN useradd -ms /bin/bash aletheia
RUN chown -R aletheia /opt/app
USER aletheia


FROM base AS celery-beat
CMD ["celery", "-A", "aletheia", "beat", "-l", "WARNING"]


FROM base AS celery-worker
CMD ["celery", "-A", "aletheia", "worker", "-l", "WARNING"]


FROM base AS django
ENV PORT 8000
EXPOSE $PORT
ENTRYPOINT [ "./entrypoint.sh" ]
CMD [ "uvicorn", "aletheia.asgi:application", "--host", "0.0.0.0", "--port", $PORT ]


FROM django AS ci-django
RUN pip install -Ur requirements.ci.txt
