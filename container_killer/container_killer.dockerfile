FROM python:3.9.7-slim

RUN apt-get update
RUN python3 -m pip install docker

COPY ./container_killer/container_killer.py container_killer.py
COPY ./containers_list.config containers_list.config


CMD ["python3", "container_killer.py"]