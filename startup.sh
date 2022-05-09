#!/bin/bash

set -a
. .env
set +a


cd src/main/python

echo "starting web app"
uvicorn main:app --host 0.0.0.0 --port 8000 --log-level debug 2>&1 &

sleep 3

echo "starting redis provider worker"
python -u provider_worker.py 2>&1 &

echo "starting redis tool worker"
python -u tool_worker.py 2>&1
