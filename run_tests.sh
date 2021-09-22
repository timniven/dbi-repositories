#!/bin/bash

docker-compose -f docker/tests.yml up

docker-compose -f docker/tests.yml down
