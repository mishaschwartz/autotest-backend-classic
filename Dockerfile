FROM python:3.10-slim as base

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt && rm requirements.txt

CMD rq worker -c rq_settings

FROM base as dev
# same as base

FROM base as prod

COPY . .
