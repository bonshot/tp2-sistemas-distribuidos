FROM python:3.9.7-slim

RUN python3 -m pip install pika

COPY ./lib/middleware.py middleware.py
COPY ./lib/communications.py communications.py
COPY ./lib/serialization.py serialization.py
COPY ./lib/logger.py logger.py
COPY ./lib/worker_class.py worker_class.py
COPY ./query_coordinator/query_coordinator.py query_coordinator.py
COPY ./query_coordinator/query_coordinator_worker.py query_coordinator_worker.py

CMD ["python3", "query_coordinator_worker.py"]