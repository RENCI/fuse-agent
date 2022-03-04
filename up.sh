#!/bin/bash

export $(cat .env|grep -v '^#')

docker-compose -f docker-compose.yml up --build -V -d
