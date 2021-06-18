FROM python:3.8-slim-buster

RUN apt-get update && apt-get install --yes gcc libpq-dev virtualenv

RUN mkdir -p /app
COPY $PWD/requirements.txt /app
WORKDIR /app

RUN virtualenv venv-py3 --python=/usr/local/bin/python3
RUN /app/venv-py3/bin/pip install --upgrade pip
RUN /app/venv-py3/bin/pip install -r requirements.txt