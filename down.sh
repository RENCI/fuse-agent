#!/bin/bash

export $(cat .env|grep -v '^#')

docker-compose -f docker-compose.yml down --remove-orphans
