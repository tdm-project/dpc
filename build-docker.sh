#!/usr/bin/env bash

mkdir -p docker/dpc
cp ingest.py docker/dpc
cp -r sources docker/dpc
cp requirements.txt docker/dpc
docker build -t tdmproject/dpc_ingestor -f docker/Dockerfile docker
