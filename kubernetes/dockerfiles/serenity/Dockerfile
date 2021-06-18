ARG BASE_IMAGE_NAME="localhost:5000/cloudwallcapital/serenity-base:latest"
FROM ${BASE_IMAGE_NAME}

COPY $PWD/src /app
COPY $PWD/etc /etc
COPY $PWD/strategies /strategies

ENV PYTHONPATH "${PYTHONPATH}:/app"
ENV LUIGI_CONFIG_PATH "/etc/luigi.cfg"