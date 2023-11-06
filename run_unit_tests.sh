#!/usr/bin/env bash
TAG=currency-exchange-unit-tests
docker build --file Dockerfile.tests --tag $TAG .
mkdir -p coverage
docker run --rm --volume="$PWD/coverage/":/var/coverage/ $TAG

