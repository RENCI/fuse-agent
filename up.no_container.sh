#!/bin/bash

# use this for debugging because
# WSL2 doesn't well support the '--network host' feature on docker

set -a
. .env
set +a

export CONFIG_PATH=./config/config.dev.no_container.json
export MONGO_CLIENT=mongodb://localhost:${MONGO_PORT}/test
export REDIS_HOST=localhost
# store data one level up so docker build context doesn't find it
export RELATIVE_DATA_PATH=../data.nocontainer

python main.py
