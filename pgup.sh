#!/bin/bash

docker-compose -f docker/pg.yml up

docker-compose -f docker/pg.yml down
