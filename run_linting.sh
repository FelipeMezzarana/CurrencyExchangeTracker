#!/bin/sh
# Usage: ./run_linting.sh [tox options, e.g. -e typing to only run typing]
TAG=currency-exchange-linting
docker build -f Dockerfile.linting -t $TAG .
docker run --rm $TAG tox $@
