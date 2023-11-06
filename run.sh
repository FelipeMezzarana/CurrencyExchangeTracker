#!/bin/sh

TAG=currency-exchange
docker build -f Dockerfile -t $TAG .
docker run --volume="./src/modules/":/src/modules \
 --volume="./src/reports/":/src/reports $TAG 
 