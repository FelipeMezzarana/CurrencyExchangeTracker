#!/usr/bin/env bash
TAG=currency-exchange-integration-tests
docker build --file Dockerfile.integration_tests --tag $TAG .
docker run --rm $TAG