FROM python:3.8-slim-buster

RUN apt-get update && apt-get install --yes gcc libpq-dev virtualenv

COPY $PWD/src /app
COPY $PWD/strategies /strategies

COPY $PWD/requirements.txt /app
WORKDIR /app

RUN /bin/sh -c virtualenv venv-py3 --python=python3
RUN /app/venv-py3/pip install --upgrade pip
RUN /app/venv-py3/pip install -r requirements.txt

ENV PYTHONPATH "${PYTHONPATH}:/app"
