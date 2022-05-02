#!/bin/bash

set -a
. .env
set +a


cd src/main/python

echo "starting web app"
uvicorn main:app --host 0.0.0.0 --port 8000 --log-level debug 2>&1 &

sleep 3

echo "starting redis worker"
python -u worker.py 2>&1
