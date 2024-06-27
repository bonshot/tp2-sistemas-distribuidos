FROM python:3.9.7-slim

RUN apt-get update
RUN python3 -m pip install docker

COPY ./lib/communications.py communications.py
COPY ./lib/healthchecking.py healthchecking.py
COPY ./container_coordinator/container_coordinator.py container_coordinator.py
COPY ./containers_list.config containers_list.config



CMD ["python3", "container_coordinator.py"]