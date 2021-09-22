#!/bin/bash

docker build \
    --no-cache \
    -f docker/dbi_repositories.Dockerfile \
    -t timniven/dbi-repositories:latest \
    .
