FROM python:3.9.7-slim

RUN python3 -m pip install pika
RUN python3 -m pip install textblob

COPY ./lib/serialization.py serialization.py
COPY ./lib/middleware.py middleware.py
COPY ./lib/filters.py filters.py
COPY ./lib/workers.py workers.py
COPY ./lib/logger.py logger.py
COPY ./lib/worker_class.py worker_class.py


COPY ./workers/global_decade_counter_worker.py global_decade_counter_worker.py

CMD ["python3", "global_decade_counter_worker.py"]
