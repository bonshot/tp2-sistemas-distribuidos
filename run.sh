#!/bin/bash

# Delete the files that contained the states of the previous run
rm -rf persisted_data/*

# Run the system
docker compose -f docker-compose-test.yaml up --build --remove-orphans