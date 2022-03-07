from datetime import datetime, timedelta
import dateutil.parser
import inspect
import os

from fastapi import FastAPI, File, UploadFile, Form, Depends, HTTPException, Query
from fastapi.logger import logger
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr, AnyUrl, Field, HttpUrl
from email_validator import validate_email, EmailNotValidError
from typing import Type, Optional, List, Union
from enum import Enum
from starlette.responses import StreamingResponse

from bson.json_util import dumps, loads

import traceback

from logging.config import dictConfig
import logging
from fuse.models.Config import LogConfig

dictConfig(LogConfig().dict())
logger = logging.getLogger("fuse-agent")


def as_form(cls: Type[BaseModel]):
    new_params = [
        inspect.Parameter(
            field.alias,
            inspect.Parameter.POSITIONAL_ONLY,
            default=(Form(field.default) if not field.required else Form(...)),
        )
        for field in cls.__fields__.values()
    ]

    async def _as_form(**data):
        return cls(**data)

    sig = inspect.signature(_as_form)
    sig = sig.replace(parameters=new_params)
    _as_form.__signature__ = sig
    setattr(cls, "as_form", _as_form)
    return cls

class PCAParameters(BaseModel):
    NumberPrincipalComponents: int = 3

class CellularFunctionParameters(BaseModel):
    SampleNumber: int = 32
    Ref: str = "MT_recon_2_2_entrez.mat"
    ThreshType: str = "local"
    PercentileOrValue: str = "value"
    Percentile: int = 25
    Value: int = 5
    LocalThresholdType: str = "minmaxmean"
    PercentileLow: int = 25
    PercentileHigh: int = 75
    ValueLow: int = 5
    ValueHigh: int = 5

# xxx fit this to known data provider parameters
@as_form
class AnalysisParameters(BaseModel):
    Union[PCAParameters,
          CellularFunctionParameters]


class Checksums(BaseModel):
    checksum: str
    type: str
    
class AccessURL(BaseModel):
    url: AnyUrl=None
    headers: str=None
    
class AccessMethods(BaseModel):
    type: str=None
    access_url: AccessURL = None
    access_id: str=None
    region: str=None
    
class Contents(BaseModel):
    name: str=None
    id: str=None
    drs_uri: AnyUrl=None
    contents: List[str] = []


class DataType(str, Enum):
    dataset_geneExpression = 'dataset-geneExpression'
    results_PCA = 'results-PCA'
    results_CellularFunction = 'results-cellularFunction'
    # xxx to add more datatypes: expand this

class JobStatus(str, Enum):
    started='started'
    failed='failed'
    finished='finished'

class Service(BaseModel):
    id: str
    title: str = None
    URL: HttpUrl = None
    

class ProviderParameters(BaseModel):
    submitter_id: EmailStr = Field(...,
                                   title="email",
                                   description="unique submitter id (email)")
    service_id: str =        Field(...,
                                   title="Provider service id",
                                   description="id of service used to upload this object")
    data_type: DataType =    Field(...,
                                   title="Type of data",
                                   descripton="informs the client how to solicit/render this data")
    description: str =       Field(title="Description",
                                   description="detailed description of this data (optional)")
    group_id: str =          Field(title="External accession ID",
                                   description="if sourced from a 3rd party, this is the accession ID on that db")
    apikey: str =            Field(title="External apikey",
                                   description="if sourced from a 3rd party, this is the apikey used for retrieval")
    
@as_form
class ProviderObject(BaseModel): # xxx customize this code
    parameters: ProviderParameters = Field(...,
                                           title="Parameters",
                                           description="parameters used with provider to create this object")
    status: JobStatus =      Field(title="Job status",
                                   description="object is incomplete until status is 'finished'")
    # --
    id: str = Field(...)
    name: str
    self_uri: AnyUrl =       Field(...,
                                   title="Link to data",
                                   descripton="use this link to retrieve the bytes when JobStatus is finished")
    size: int =              Field(...,
                                   title="Size of data",
                                   descripton="size of the data, 'None' until JobStatus is finished")
    created_time: datetime = Field(datetime.utcnow(),
                                   title="Time this metadata record was created",
                                   descripton="")
    # xxx stopped here
    updated_time: datetime = None
    version: str="1.0"
    mime_type: str="application/json"
    checksums: List[Checksums] = []
    access_methods: List[AccessMethods] = []
    contents: List[Contents] = []
    aliases: List[str] = []

@as_form
class ResultsObject(BaseModel): # xxx customize this code
    source_data: ProviderObject = None
    parameters: AnalysisParameters = None
    results: ProviderObject = None

class SubmitterStatus(str, Enum):
    requested='requested'
    approved='approved'
    disabled='disabled'

@as_form
class Submitter(BaseModel):
    object_id: str = None
    submitter_id: EmailStr = None
    created_time: datetime = None
    status: SubmitterStatus = SubmitterStatus.requested
    
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

import pymongo
mongo_client = pymongo.MongoClient('mongodb://%s:%s@agent-tx-persistence:%s/test' % (os.getenv('MONGO_NON_ROOT_USERNAME'), os.getenv('MONGO_NON_ROOT_PASSWORD'),os.getenv('MONGO_PORT')))

mongo_db = mongo_client.test
mongo_agent=mongo_db.agent
mongo_submitters=mongo_db.submitters

import pathlib
import json
@app.get("/config", summary="Returns the config for the appliance")
async def config():
    '''
    once you have the list of appliances, you can call each one separately to get the descriptoin of parameters to give to end-users;
    for example, this to get the full schema forthe parameters required for submitting an object to be loaded by a data provider:
    curl -X 'GET' '${provider_host}:${provider_port}/openapi.json' -H 'accept: application/json'   2> /dev/null |jq '.paths."/submit".post.parameters'
    xxx this could also be done by the agent over a docker network if all containers are on the same machine; from inside any container:
    curl -X 'GET' 'fuse-provider-upload:8000/openapi.json
    '''
    config_path = pathlib.Path(__file__).parent / "config.json"
    with open(config_path) as f:
        return json.load(f)

# xxx get with David to find out what else this should return in the json
@app.get("/providers", summary="Returns a list of the configured data providers")
async def providers():
    config_path = pathlib.Path(__file__).parent / "config.json"
    with open(config_path) as f:
        config= json.load(f)
    return config["configuredServices"]["providers"]


@app.get("/tools", summary="Returns a list of the configured data tools")
async def tools():
    config_path = pathlib.Path(__file__).parent / "config.json"
    with open(config_path) as f:
        config= json.load(f)
    return config["configuredServices"]["tools"]


def _submitter_object_id(submitter_id):
    return "agent_" + submitter_id 

@app.post("/add/submitter", summary="Create a record for a new submitter")
async def add_submitter(submitter_id: str = Query(default=None, description="unique identifier for the submitter (e.g., email)")):
    '''
    Add a new submitter
    '''
    try:
        object_id = _submitter_object_id(submitter_id)
        entry = mongo_submitters.find({"object_id": object_id},
                                      {"_id": 0, "submitter_id": 1})
        logger.info(msg=f"[add_submitter]found ({entry.count()}) matches for object_id={object_id}")
        if entry.count() != 0:
            raise Exception(f"Submitter already added as: {object_id}, entries found = {entry.count()}")

        submitter_object = Submitter(
            object_id = object_id,
            submitter_id = submitter_id,
            created_time = datetime.utcnow(),
            status = SubmitterStatus.approved)

        logger.info(msg=f"[add_submitter] submitter_object={submitter_object}")
        mongo_submitters.insert(submitter_object.dict())
        logger.info(msg="[add_submitter] submitter added.")

        ret_val = {"submitter_id": submitter_id}
        logger.info(msg=f"[add_submitter] returning: {ret_val}")
        return ret_val
    except Exception as e:
        logger.info(msg=f"[add_submitter] exception, setting upload status to failed for {object_id}")
        raise HTTPException(status_code=404,
                            detail=f"! Exception {type(e)} occurred while inserting submitter ({submitter_id}), message=[{e}] \n! traceback=\n{traceback.format_exc()}\n")
        

@app.get("/submitter/{submitter_id}", summary="Return metadata associated with submitter")
async def get_submitter(submitter_id: str = Query(default=None, description="unique identifier for the submitter (e.g., email)")):
    try:
        object_id = _submitter_object_id(submitter_id)
        entry = mongo_submitters.find({"object_id": object_id},{"_id":0})
        logger.info(msg=f"[submitter]found ({entry.count()}) matches for object_id={object_id}")
        if entry.count() != 1:
            raise Exception(f"Wrong number of submitters found for [{object_id}], entries found = {entry.count()}")
        ret_val = entry[0]
        logger.info(msg=f"[submitter] returning: {ret_val}")
        return ret_val
    except Exception as e:
        raise HTTPException(status_code=404,
                            detail=f"! Exception {type(e)} occurred while finding submitter ({submitter_id}), message=[{e}] \n! traceback=\n{traceback.format_exc()}\n")    
    
@app.get("/search/submitters", summary="Return a list of known submitters")
async def get_submitters(within_minutes: Optional[int] = Query(default=None, description="find submitters created within the number of specified minutes from now")):
    '''
    return list of submitters
    '''
    try:
        if within_minutes != None:
            logger.info(msg=f"[submitters] get submitters created within the last {within_minutes} minutes.")
            until_time = datetime.utcnow()
            within_minutes_time = timedelta(minutes=within_minutes)
            from_time = until_time - within_minutes_time
            search_object = {
                "created_time": {
                    "$gte": from_time,
                    "$lt": until_time
                }
            }
        else:
            logger.info(msg="[submitters] get all.")
            search_object = {}
        ret = list(map(lambda a: a, mongo_submitters.find(search_object, {"_id": 0, "submitter_id": 1})))
        logger.info(msg=f"[submitters] ret:{ret}")
        return ret
    
    except Exception as e:
        raise HTTPException(status_code=404,
                            detail=f"! Exception {type(e)} occurred while searching submitters, message=[{e}] \n! traceback=\n{traceback.format_exc()}\n")
    
@app.delete("/delete/submitter/{submitter_id}", summary="Remove a submitter record")
async def delete_submitter(submitter_id: str= Query(default=None, description="unique identifier for the submitter (e.g., email)")):
    '''
    deletes submitter and their datasets, analyses
    '''
    delete_status = "done"
    ret_mongo=""
    ret_mongo_err=""
    try:
        logger.warn(msg=f"[delete_submitter] Deleting submitter_id:{submitter_id}")
        ret = mongo_submitters.delete_one({"submitter_id": submitter_id})
        #<class 'pymongo.results.DeleteResult'>
        delete_status = "deleted"
        if ret.acknowledged != True:
            delete_status = "failed"
            ret_mongo += "ret.acknoledged not True.\n"
            logger.error(msg="[delete_submitter] delete failed, ret.acknowledged ! = True")
        if ret.deleted_count != 1:
            # should never happen if index was created for this field
            delete_status = "failed"
            ret_mongo += f"Wrong number of records deleted ({ret.deleted_count})./n"
            logger.error(msg=f"[delete_submitter] delete failed, wrong number deleted, count[1]={ret.deleted_count}")
        ## xxx
        # could check if there are any remaining; but this should instead be enforced by creating an index for this columnxs
        # could check ret.raw_result['n'] and ['ok'], but 'ok' seems to always be 1.0, and 'n' is the same as deleted_count
        ##
        ret_mongo += f"Deleted count=({ret.deleted_count}), Acknowledged=({ret.acknowledged})./n"
    except Exception as e:
        logger.error(msg=f"[delete_submitter] Exception {type(e)} occurred while deleting {submitter_id} from database, message=[{e}]\n")
        ret_mongo_err += f"! Exception {type(e)} occurred while deleting {submitter_id}) from database, message=[{e}] \n! traceback=\n{traceback.format_exc()}\n"
        delete_status = "exception"
        
    ret = {
        "status": delete_status,
        "info": ret_mongo,
        "stderr": ret_mongo_err
    }
    logger.info(msg=f"[delete_submitter] returning ({ret})\n")
    return ret

@app.get("/provider_parameters", summary="convenience function to get the submit parameters for a provider service")
async def get_submit_parameters(service_id: str = Query(default="fuse-provider-upload", describe="loop through /providers to retrieve the submit parameters for each, allowing the end user to load in datasets as desired")):
    '''
    Only works for containers on the same 'fuse-agent_fuse' network as this agent; consequently, containers must be hosted on the same machine.
    '''
    import requests
    response = requests.get(f"http://{service_id}:8000/openapi.json")# xxx won't work, needs to be on same docker network!
    json_obj = response.json()
    return json_obj['paths']['/submit']['post']['parameters'] # prints the string with 'source_name' key



    
# xxx can we build an enum class dynamically from the providers list?
#@app.post("/load", summary="load a dataset for analysis")
#async def load(parameters: ProviderParameters,
#               files: Optional[List[UploadFile]] = File(...),
#               ):
    '''
    params:
     - provider url
     - provider parameter values
    returns object_id
 '''

@app.get("/objects/{object_id}", summary="get metadata for the object")
async def get_object():
    '''
    gets object metadata, including link to object
    Includes status and links to the input dataset, parameters, and dataset results
    '''

@app.get("/search/objects", summary="get all object metadata accessible for this submitter")
async def get_results(submitter_id: Optional[str] = Query(default=None, description="unique identifier for the submitter (e.g., email)")):
    '''
    takes optional parameter "data_type=<data-type>" from [dataset-geneExpression, result-PCA, result-cellularFunction]
    for dataset type objects, metadata would include provider url, provider object_id
    for results type objects, metadata would include tool parameter values, tool url, provider url, provider object_id
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
from slack:
When an analysis is submitted to an agent, the agent will:
Create a "results"-type object_id in it's database, containing status="started" and the link to where you can get the object when its finished
enqueue the tool request
return the object_id
2. When the analysis job comes up, the agent updates the status, calls the tool, waits for a result,  and persists the result
3. When the dashboard asks for the object, the agent returns the meta data
4. If the meta data shows status = finished, the dashboard uses the link in the meta data to retrieve the results. (edited) 
    '''


