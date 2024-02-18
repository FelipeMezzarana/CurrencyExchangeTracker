#!/bin/sh

TAG=currency-exchange
docker build -f Dockerfile -t $TAG .
docker run --volume="./src/database/":/src/database \
 --volume="./src/reports/":/src/reports $TAG 
 