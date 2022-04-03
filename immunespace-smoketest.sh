#!/bin/bash

x=`curl -X 'POST' \
 'http://localhost:8081/objects/load' \
 -H 'accept: application/json' \
 -H 'Content-Type: multipart/form-data' \
 -F 'optional_file_expression=@t/input/expression.csv;type=application/vnd.ms-excel' \
 -F 'optional_file_properties=@t/input/phenotypes.csv;type=application/vnd.ms-excel' \
 -F 'service_id=fuse-provider-immunespace' \
 -F 'submitter_id=krobasky@renci.org' \
 -F 'data_type=class_dataset_expression' \
 -F 'accession_id=test-SDY61-9' \
 -F 'apikey=apikey|1a4cfbff854d60e6b65af863402b988d' 2> /dev/null |jq .object_id|sed 's/"//g'`
curl -X 'GET' http://localhost:8081/objects/${x} -H 'accept: application/json' 2> /dev/null | jq .
