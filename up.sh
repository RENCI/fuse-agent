#!/bin/bash

export $(cat .env|grep -v '^#')
sudo chown -R ${USER} data
docker-compose -f docker-compose.yml up --build -V -d
