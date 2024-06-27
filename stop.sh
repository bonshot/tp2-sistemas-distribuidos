#!/bin/bash
docker compose -f docker-compose-dev.yaml stop -t 5
docker compose -f docker-compose-dev.yaml down