FROM python:3.9.7-slim

RUN python3 -m pip install pika

COPY ./lib/middleware.py middleware.py
COPY ./lib/communications.py communications.py
COPY ./lib/serialization.py serialization.py
COPY ./lib/logger.py logger.py
COPY ./server/server.py server.py

CMD ["python3", "server.py"]