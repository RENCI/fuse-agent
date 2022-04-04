#!/bin/bash


SUBMITTER="krobasky@renci.org"
ACCESSION_ID=test-SDY61-9
APIKEY="apikey|1a4cfbff854d60e6b65af863402b988d"

dataset=`curl -X 'POST' \
 'http://localhost:8081/objects/load' \
 -H 'accept: application/json' \
 -H 'Content-Type: multipart/form-data' \
 -F 'optional_file_expression=@t/input/expression.csv;type=application/vnd.ms-excel' \
 -F 'optional_file_properties=@t/input/phenotypes.csv;type=application/vnd.ms-excel' \
 -F 'service_id=fuse-provider-immunespace' \
 -F "submitter_id=${SUBMITTER}" \
 -F 'data_type=class_dataset_expression' \
 -F "accession_id=${ACCESSION_ID}" \
 -F "apikey=${APIKEY}" 2> /dev/null |jq .object_id|sed 's/"//g'`
curl -X 'GET' http://localhost:8081/objects/${dataset} -H 'accept: application/json' 2> /dev/null | jq . > out_dataset_meta.json
echo "dataset=$dataset"
result=`curl -X 'POST' \
  'http://localhost:8081/analyze' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d "service_id=fuse-tool-pca&number_of_components=3&results_provider_service_id=fuse-provider-upload&submitter_id=krobasky%40renci.org&dataset=${dataset}" 2> /dev/null |jq .object_id|sed 's/"//g'`
echo "result=${result}"
sleep 2
curl -X 'GET' http://localhost:8081/objects/${result} -H 'accept: application/json' 2> /dev/null |jq . > out_result_meta.json

result_url=`curl -X 'GET' "http://localhost:8081/objects/url/${result}/type/filetype_results_PCATable" -H 'accept: application/json' 2> /dev/null |jq .url|sed 's/"//g'`
echo "result_url=${result_url}"
curl -X 'GET' ${result_url} 2> /dev/null |jq . > out_result.json
