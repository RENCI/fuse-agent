import datetime
import inspect
import os

from fastapi import FastAPI, File, UploadFile, Form, Depends, HTTPException, Query
from fastapi.logger import logger
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Type, Optional, List
from starlette.responses import StreamingResponse

from bson.json_util import dumps, loads
import docker

from docker.errors import ContainerError
import traceback

app = FastAPI()

origins = [
    f"http://{os.getenv('HOSTNAME')}:{os.getenv('HOSTPORT')}",
    f"http://{os.getenv('HOSTNAME')}",
    "http://localhost:{os.getenv('HOSTPORT')}",
    "http://localhost",
    "*",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

import pathlib
import json
@app.get("/config", summary="Returns the config for the appliance")
async def config():
    config_path = pathlib.Path(__file__).parent / "config.json"
    with open(config_path) as f:
        return json.load(f)

# xxx get with David to find out what else this should return in the json
@app.get("/providers", summary="Returns a list of the configured data providers")
async def providers():
    config_path = pathlib.Path(__file__).parent / "config.json"
    with open(config_path) as f:
        config= json.load(f)
    return config.configuredServices.providers


@app.get("/tools", summary="Returns a list of the configured data tools")
async def tools():
    config_path = pathlib.Path(__file__).parent / "config.json"
    with open(config_path) as f:
        config= json.load(f)
    return config.configuredServices.tools

@app.post("/submitter", summary="Create a record for a new submitter")
async def add_submitter():
    '''
    adds user
    '''

@app.get("/submitters", summary="Return a list of all known submitters")
async def get_submitters():
    '''
    adds user
    '''

@app.delete("/submitter/{submitter_id}", summary="Remove a submitter record")
async def delete_submitter():
    '''
    deletes submitter and their datasets, analyses
    '''

@app.post("/load", summary="load a dataset for analysis")
async def load():
    '''
    params:
     - provider url
     - provider parameter values
    returns object_id
 '''

@app.post("/analyze", summary="submit an analysis")
async def analyze():
    '''
    params: 
     - provider url 
     - provider object_id 
     - tool url
     - tool parameter values
    returns results_id
    '''
    

@app.get("/search/{submitter_id}", summary="get all object metadata accessible for this submitter")
async def get_results():
    '''
    takes optional parameter "data_type=<data-type>" from [dataset-geneExpression, result-PCA, result-cellularFunction]
    for dataset type objects, metadata would include provider url, provider object_id
    for results type objects, metadata would include tool parameter values, tool url, provider url, provider object_id
    '''

@app.get("/objects/{object_id}", summary="get metadata for the object")
async def get_object():
    '''
    gets object metadata, including link to object
    Includes status and links to the input dataset, parameters, and dataset results
    '''

