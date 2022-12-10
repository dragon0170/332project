#!/bin/bash

docker-compose down -v

docker build . --platform linux/amd64 -f gensort.Dockerfile -t gensort:0.1

sbt docker:stage
cp target/docker/stage/Dockerfile target/docker/stage/master
cp target/docker/stage/Dockerfile target/docker/stage/slave

sed -i '' 's/distributedsort/master/' target/docker/stage/master
sed -i '' 's/distributedsort/slave/' target/docker/stage/slave

cd target/docker/stage
docker build . -f master -t master:0.1
docker build . -f slave -t slave:0.1

docker-compose up
