#!/bin/bash

# Delete the files that contained the states of the previous run
rm -rf persisted_data/*

# Run the system
export COMPOSE_HTTP_TIMEOUT=230
docker compose -f docker-compose-dev.yaml up --build --remove-orphans