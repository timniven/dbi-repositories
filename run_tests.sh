#!/bin/bash

# docker-compose -f docker/tests.yml up

docker compose \
    -f docker/tests.yml \
        run \
            dbi-repositories \
                python -m unittest discover
#docker compose -f docker/tests.yml up
docker compose -f docker/tests.yml down
