import uvicorn
from datetime import datetime, timedelta
import dateutil.parser
import inspect
import os
import shutil

from fastapi import FastAPI, File, UploadFile, Form, Depends, HTTPException, Query
from fastapi.logger import logger
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr, AnyUrl, Field, HttpUrl, AnyHttpUrl
from email_validator import validate_email, EmailNotValidError
from typing import Type, Optional, List, Union, Dict
from enum import Enum
from starlette.responses import StreamingResponse

import aiofiles
import uuid
import requests
import pathlib
import json

import nest_asyncio
nest_asyncio.apply()

#from bson.json_util import dumps, loads

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


# xxx clean up these schemas and pull them out to the fuse.models diretory
# xxx fit this to known data provider parameters


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


class JobStatus(str, Enum):
    started='started'
    failed='failed'
    finished='finished'

class Service(BaseModel):
    id: str
    title: str = None
    URL: HttpUrl = None

class SubmitterActionStatus(str, Enum):
    unknown='unknown' 
    created='created' 
    existed='existed' 
    
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
    
tags_metadata = [
    {"name": "Data Provider Service", "description": "Call out to 3rd party data provider services"},
    {"name": "Tool Service", "description": "Call out to 3rd party tool services (may query a data provider along the way)"},
    {"name": "Submitter", "description": "Manage users"},
    {"name": "Service", "description": "Tools and provider methods"},
    {"name": "Get", "description": "Query only: All the methods you can use with a 'get' http call"},
    {"name": "Post", "description": "Updata persistent data: All the methods you can use with a 'post' http call"},
    {"name": "Delete", "description": "WARNING: Only use these calls for redacting data, correcting an inconsistent database state, or removing test data."}
]
g_api_version="0.0.1"

app = FastAPI(openapi_url=f"/api/{g_api_version}/openapi.json",
              title="Fuse Agent",
              description="Agent for orchestrating configurable data sources and tools",
              version=g_api_version,
              terms_of_service="https://github.com/RENCI/fuse-agent/doc/terms.pdf",
              contact={
                  "name": "Maintainer(Kimberly Robasky)",
                  "url": "http://txscience.renci.org/contact/",
                  "email": "kimberly.robasky@gmail.com"
            },
            license_info={
            "name": "MIT License",
                "url": "https://github.com/RENCI/fuse-agent/blob/main/LICENSE"
            }
              )


origins = [
    f"http://{os.getenv('HOST_NAME')}:{os.getenv('HOST_PORT')}",
    f"http://{os.getenv('HOST_NAME')}",
    "http://localhost:{os.getenv('HOST_PORT')}",
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
g_mongo_client_str = os.getenv("MONGO_CLIENT")
logger.info(msg=f"[MAIN] connecting to {g_mongo_client_str}")
mongo_client = pymongo.MongoClient(g_mongo_client_str)

mongo_db = mongo_client.test
mongo_db_version = mongo_db.command({'buildInfo':1})['version']
mongo_db_major_version = mongo_client.server_info()["versionArray"][0]
mongo_db_minor_version = mongo_client.server_info()["versionArray"][1]
mongo_agent=mongo_db.agent
mongo_submitters=mongo_db.submitters
mongo_objects=mongo_db.objects

# mongo migration functions to support running outside of container with more current instance
def _mongo_insert(coll, obj):
        if mongo_db_major_version < 4:
            logger.info(msg=f"[_mongo_insert] using collection.insert")
            coll.insert(obj)
        else:
            logger.info(msg=f"[mongo_insert] using collection.insert_one")
            coll.insert_one(obj)

def _mongo_count(coll, obj):
    if mongo_db_major_version < 3 and mongo_db_minor_version < 7:
        logger.info(msg=f"[_mongo_count] mongodb version = {mongo_db_version}, use deprecated entry count function")
        entry = coll.find(obj, {})
        num_matches= entry[0].count()
    else: 
        logger.info(msg=f"[_mongo_count] mongo_db version = {mongo_db_version}, use count_documents function")
        num_matches=coll.count_documents(obj)
    logger.info(msg=f"[_mongo_count]found ({num_matches}) matches")
    return num_matches
# end mongo migration functions
            

def _read_config():
    function_name="[_read_config]"
    logger.info(msg=f'{function_name} CONFIG_PATH={os.getenv("CONFIG_PATH")}')
    config_path = pathlib.Path(__file__).parent / os.getenv("CONFIG_PATH")
    logger.info(msg=f'{function_name} reading config file={config_path}')
    with open(config_path) as f:
        return json.load(f)

def _get_services(prefix = ""):
    assert prefix == "fuse-provider-" or prefix == "fuse-tool-" or prefix == ""
    config = _read_config()
    return list(filter(lambda x: x.startswith(prefix), list(config["configured-services"])))
    
def _get_url(service_id: str, url_type: str = "service_url", host_type: str = "configured-services"):
    function_name="_get_url"
    config = _read_config()
    logger.info(msg=f'{function_name} service_id={service_id}, url_type={url_type}, host_type={host_type}')
    return config[host_type][service_id][url_type]

def _submitter_object_id(submitter_id):
    return "agent_" + submitter_id 

####################
# service-info methods and cdm's
def _get_service_info(service_id):
    config = _read_config()
    response = requests.get(f'{config["configured-services"][service_id]["service_url"]}/service-info')
    return response.json()

class ServiceIOType(str, Enum):
    datasetInput='inputDatasetType' 
    resultsOutput='outputResultsType'

class ServiceIOField(str, Enum):
    dataType='data_type' 
    fileTypes='file_types'
    fileType='file_type'
    mimeType='mime_type'
    fileExt='file_extension'

def _get_service_value(service_id, iotype: ServiceIOType, field: ServiceIOField):
    assert service_id.startswith("fuse-tool-")
    service_info = _get_service_info(service_id)
    return service_info[iotype][field]
####################


# xxx get with David to find out what else this should return in the json
@app.get("/services/providers", summary="Returns a list of the configured data providers", tags=["Get","Service","Data Provider Service"])
async def providers():
    return _get_services("fuse-provider-")

@app.get("/services/tools", summary="Returns a list of the configured data tools", tags=["Get","Service","Tool Service"])
async def tools():
    return _get_services("fuse-tool-")


def _resolveRef(ref,models):
    (refpath, model_name) = os.path.split(ref["$ref"])
    logger.info(msg=f"[_resolveRef] referenced path={refpath}, model={model_name} ")
    _resolveRefs(models[model_name], models)
    return (model_name)

def _resolveRefs(doc, models):
    if type(doc) == dict:
        if "$ref" in doc:
            model_name =_resolveRef(doc, models)
            doc[model_name] = models[model_name]
            del doc["$ref"]
            logger.info(msg=f"[_resolveRefs] STOP:resolved[name={model_name}, obj={doc[model_name]}]")
            return
        else:
            for k,v in doc.items():
                logger.info(msg=f"[_resolveRefs] resolving dict key:{k}, value:{v}")
                _resolveRefs(doc[k], models)
    elif type(doc) == list:
        for elem in doc:
            logger.info(msg=f"[_resolveRefs] resolving list element {elem}")
            _resolveRefs(elem, models)
    else:
        logger.info(msg=f"[_resolveRefs] STOP:doc type ({type(doc)}) for leaf doc={doc}")
        return

                
@app.get("/services/result_types/{service_id}", summary="types of results supported by this tool service")
async def get_tool_result_types(service_id: str = Query(default="fuse-tool-pca", describe="loop through /tools to retrieve the results types for each, providing the dashboard with everything it needs to render forms and solicit all the necessary information from the end user in order to analyze results")):
    '''
    so far, known values are:
    result-type-pcaTable
    result-type-cellfie
    '''
    raise HTTPException(status_code=404,
                        detail=f"! get_tool_result_types under construction")
    

# xxx add this to systems tests after all subsystems are integrated
@app.get("/services/schema/{service_id}", summary="returns the schema for the submit parameters required by the given service", tags=["Get","Service","Data Provider Service","Tool Service"])
async def get_submit_parameters(service_id: str = Query(default="fuse-provider-upload", describe="loop through /providers or /tools to retrieve the submit parameters for each, providing the dashboard with everything it needs to render forms and solicit all the necessary information from the end user in order to load in datasets and/or run analyses"),
                                version: str = Query(default="0.0.1", describe="version of the api to use, 0.0.1 by default")):
    try:
        function_name="get_submit_parameters"
        host_url = _get_url(f'{service_id}')
        openapi_url = f'{host_url}/api/{version}/openapi.json'
        logger.info(msg=f'{function_name} openapi_url={openapi_url}')
        response = requests.get(f"{openapi_url}")
        json_obj = response.json()
        logger.info(msg=f'{function_name} response={json_obj}')
        params = json_obj['paths']['/submit']['post']['parameters'] 
        components = json_obj['components']['schemas']
        # look for any referenced data models and fill them in
        # this is helpful: https://swagger.io/docs/specification/using-ref/
        #                  https://stackoverflow.com/questions/60010686/how-do-i-access-the-ref-in-the-json-recursively-as-to-complete-the-whole-json
        # Recurses through dictionaries, lists, and nested references; doesn't handle referenced files
        logger.info(msg=f"[get_submit_parameters] resolving submit params={params}")
        logger.info(msg=f"[get_submit_parameters] components={components}")
        _resolveRefs(params, components)
        return params
    except Exception as e:
        raise HTTPException(status_code=500,
                            detail=f"! Exception {type(e)} occurred while retrieving input schema for service submit, message=[{e}] ! traceback={traceback.format_exc()}")

@app.get("/services", summary="Returns a list of all configured services", tags=["Get","Service","Data Provider Service","Tool Service"])
async def all_services():
    '''
    once you have the list of services, you can call each one separately to get the descriptoin of parameters to give to end-users;
    for example, this to get the full schema forthe parameters required for submitting an object to be loaded by a data provider:
    /services/schema/{service_id}
    Aside: this is done internally by requesting the openapi.json from the fastapi-enabled service, similar to:
    curl -X 'GET' 'fuse-provider-upload:8000/openapi.json
    '''
    return _get_services("")

def _submitter_object_id(submitter_id):
    return "agent_" + submitter_id 

def api_add_submitter(submitter_id: str):
    object_id = _submitter_object_id(submitter_id)
    num_matches = _mongo_count(mongo_submitters, {"object_id": object_id})

    submitter_action_status = SubmitterActionStatus.unknown
    if num_matches == 1:
        submitter_action_status = SubmitterActionStatus.existed
    else :
        assert num_matches == 0
        submitter_object = Submitter(
            object_id = object_id,
            submitter_id = submitter_id,
            created_time = datetime.utcnow(),
            status = SubmitterStatus.approved)

        logger.info(msg=f"[api_add_submitter] submitter_object={submitter_object}")
        _mongo_insert(mongo_submitters, submitter_object.dict())
        logger.info(msg="[api_add_submitter] submitter added.")
        submitter_action_status = SubmitterActionStatus.created

    ret_val = {
        "submitter_id": submitter_id,
        "submitter_action_status": submitter_action_status
    }
    logger.info(msg=f"[api_add_submitter] returning: {ret_val}")
    return ret_val
    
    
@app.post("/submitters/add", summary="Create a record for a new submitter", tags=["Post","Submitter"])
async def add_submitter(submitter_id: str = Query(default=None, description="unique identifier for the submitter (e.g., email)")):
    '''
    Add a new submitter
    '''
    try:
        return api_add_submitter(submitter_id)
    except Exception as e:
        logger.info(msg=f"[add_submitter] exception, ! Exception {type(e)} occurred while inserting submitter ({submitter_id}), message=[{e}] ! traceback={traceback.format_exc()}")
        raise HTTPException(status_code=404,
                            detail=f"! Exception {type(e)} occurred while inserting submitter ({submitter_id}), message=[{e}] ! traceback={traceback.format_exc()}")
        
def api_get_submitters(within_minutes:int = None):
        if within_minutes != None:
            logger.info(msg=f"[api_get_submitters] get submitters created within the last {within_minutes} minutes.")
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
            logger.info(msg="[api_get_submitters] get all.")
            search_object = {}
        ret = list(map(lambda a: a, mongo_submitters.find(search_object, {"_id": 0, "submitter_id": 1})))
        logger.info(msg=f"[api_get_submitters] ret:{ret}")
        return ret

@app.get("/submitters/search", summary="Return a list of known submitters", tags=["Get","Submitter"])
async def get_submitters(within_minutes: Optional[int] = Query(default=None, description="find submitters created within the number of specified minutes from now")):
    '''
    return list of submitters
    '''
    try:
        return api_get_submitters(within_minutes)
    except Exception as e:
        raise HTTPException(status_code=404,
                            detail=f"! Exception {type(e)} occurred while searching submitters, message=[{e}] ! traceback={traceback.format_exc()}")
    
@app.delete("/submitters/delete/{submitter_id}", summary="Remove a submitter record", tags=["Delete","Submitter"])
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
            ret_mongo += "ret.acknoledged not True."
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
        logger.error(msg=f"[delete_submitter] Exception {type(e)} occurred while deleting {submitter_id} from database, message=[{e}]")
        ret_mongo_err += f"! Exception {type(e)} occurred while deleting {submitter_id}) from database, message=[{e}] ! traceback={traceback.format_exc()}"
        delete_status = "exception"
        
    ret = {
        "status": delete_status,
        "info": ret_mongo,
        "stderr": ret_mongo_err
    }
    logger.info(msg=f"[delete_submitter] returning ({ret})")
    return ret

def api_get_submitter(submitter_id):
    '''
     Expects exactly 1 match, throws exception otherwise
    '''
    object_id = _submitter_object_id(submitter_id)
    entry = mongo_submitters.find({"object_id": object_id},{"_id":0})
    num_matches = _mongo_count(mongo_submitters, {"object_id": object_id})
    logger.info(msg=f"[api_get_submitter]found ({num_matches}) matches for object_id={object_id}")
    assert num_matches == 1
    ret_val = entry[0]
    logger.info(msg=f"[api_get_submitter] returning: {ret_val}")
    return ret_val
    
@app.get("/submitters/{submitter_id}", summary="Return metadata associated with submitter", tags=["Get","Submitter"])
async def get_submitter(submitter_id: str = Query(default=None, description="unique identifier for the submitter (e.g., email)")):
    try:
        return api_get_submitter(submitter_id)
    except Exception as e:
        raise HTTPException(status_code=404,
                            detail=f"! Exception {type(e)} occurred while finding submitter ({submitter_id}), message=[{e}] ! traceback={traceback.format_exc()}")    

from multiprocessing import Process
from redis import Redis
from rq import Queue, Worker
from rq.job import Job
from rq.decorators import job
g_redis_default_timeout = os.getenv("REDIS_TIMEOUT")
g_redis_connection = Redis(host=os.getenv("REDIS_HOST"), port=os.getenv("REDIS_PORT"), db=0)
logger.info(msg=f'[MAIN] redis host={os.getenv("REDIS_HOST")}:{os.getenv("REDIS_PORT")}')
g_queue = Queue(connection=g_redis_connection, is_async=True, default_timeout=g_redis_default_timeout)
def _initWorker():
    worker = Worker(g_queue, connection=g_redis_connection)
    worker.work()

def _file_path(object_id):
    relative_data_path = os.getenv("RELATIVE_DATA_PATH")
    local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), relative_data_path)
    return os.path.join(local_path, f"{object_id}-data")

def _get_default_results_provider_service_id():
    config = _read_config()
    return config["results-provider-services"]["default"]
    
@as_form
class ToolParameters(BaseModel):
    service_id: str
    submitter_id: EmailStr = Field(...,
                                   title="email",
                                   description="unique submitter id (email)")
    number_of_components: Optional[int] = 3
    dataset: str = Field(...)
    description: Optional[str] =  Field(None, title="Description",
                                        description="detailed description of the requested analysis being performed (optional)")
    expression_url: Optional[AnyHttpUrl] = Field(None, title="Gene expression URL",
                                                      description="Optionally grab expression from an URL instead of uploading a file")
    properties_url: Optional[AnyHttpUrl] = Field(None, title="Properties URL",
                                                      description="Optionally grab properties from an URL instead of uploading a file")
    archive_url: Optional[AnyHttpUrl] = Field(None, title="Archive URL",
                                                      description="Optionally grab all the files from an URL to an archive instead of uploading file(s)")
    results_provider_service_id: Optional[str] = Field(_get_default_results_provider_service_id(), title="Data Provider for Results",
                                            description="If not set, the system default will be provided. e.g., 'fuse-provider-upload'")

# xxx cdm
from enum import Enum
class DataType(str, Enum):
    geneExpression='class_dataset_expression'
    resultsPCATable='class_results_PCATable'
    resultsCellFIE='class_results_CellFIE'
    # xxx to add more datatypes: expand this

class FileType(str, Enum):
    datasetGeneExpression='filetype_dataset_expression'
    datasetProperties='filetype_dataset_properties'
    datasetArchive='filetype_dataset_archive'
    resultsPCATable='filetype_results_PCATable'
    resultsCellFIE='filetype_results_CellFIE'
    # xxx to add more datatypes: expand this
    
    
@as_form
class ProviderParameters(BaseModel):
    service_id: str =        Field(...,
                                   title="Provider service id",
                                   description="id of service used to upload this object")
    submitter_id: EmailStr = Field(...,
                                   title="email",
                                   description="unique submitter id (email)")
    data_type: DataType = Field(..., title="Data type of this object",
                                description="the type of data associated with this object (e.g, results or input dataset)")
    description: Optional[str] =  Field(None, title="Description",
                                        description="detailed description of this data (optional)")
    version: Optional[str] =  Field(None, title="Version of this object",
                                        description="objects shouldn't ever be deleted unless data are redacted or there is a database consistency problem.")
    accession_id: Optional[str] =    Field(None, title="External accession ID",
                                        description="if sourced from a 3rd party, this is the accession ID on that db")
    apikey: Optional[str] =       Field(None, title="External apikey",
                                        description="if sourced from a 3rd party, this is the apikey used for retrieval")
    aliases: Optional[str] =       Field(None, title="Optional list of aliases for this object")
    checksums: Optional[List[Checksums]] = Field(None, title="Optional checksums for the object",
                                                 description="enables verification checking by clients; this is a json list of objects, each object contains 'checksum' and 'type' fields, where 'type' might be 'sha-256' for example.")
    

# SHARED
def _gen_object_id(prefix, submitter_id, requested_object_id, coll):
    try:
        object_id = f"{prefix}_{submitter_id}_{uuid.uuid4()}"
        assert requested_object_id != None
        logger.info(msg=f"[_gen_object_id] top prefex={prefix}, submitter={submitter_id}, requested:{requested_object_id}")
        entry = coll.find({"object_id": requested_object_id},
                          {"_id": 0, "object_id": 1})
        num_matches = _mongo_count(coll,{"object_id": requested_object_id})
        logger.info(msg=f"[_gen_object_id]found ({num_matches}) matches for requested object_id={requested_object_id}")
        assert num_matches == 0
        return requested_object_id
    except Exception as e:
        logger.warn(msg=f"[_gen_object_id] ? Exception {type(e)} occurred when using {requested_object_id}, using {object_id} instead. message=[{e}] ! traceback={traceback.format_exc()}")
        logger.warn(msg=f"[_gen_object_id] ")
        return object_id

def _set_agent_status(accession_id, service_object_id, num_files_requested, num_loaded_files):
    # status = finished if
    #      len(file_objects) = num_files_requested and
    #      if obj.parameters.accession_id is not None, then service_object_id is not None
    if num_files_requested == num_loaded_files and (accession_id is None or service_object_id is not None):
        return "finished"
    else:
        return "started"
        

    
# @job('low', connection=g_redis_connection, timeout=g_redis_default_timeout)
async def _remote_submit_file(agent_object_id:str, file_type:str, agent_file_path:str):
    try:
        function_name="[_remote_submit_file]"
        # because this runs out-of-band, or maybe the async is doing it, I think we might need a new mongodb connection?
        logger.info(msg=f"{function_name} ({file_type}) connecting to {g_mongo_client_str} anew; agent_object_id:{agent_object_id} file_type:{file_type}, agent_file_path:{agent_file_path} ")
        my_mongo_client = pymongo.MongoClient(g_mongo_client_str)
        my_mongo_db = my_mongo_client.test
        m_objects=my_mongo_db.objects
        detail_str = ""
        timeout_seconds = g_redis_default_timeout # xxx read this from config.json, what's reasonable here?
        service_object_id = None # set this early in case there's an exception
        # get the agent object for this job
        logger.info(msg=f"{function_name} ({file_type}) looking up {agent_object_id}")
        entry = m_objects.find({"object_id":agent_object_id},{"_id":0})
        assert _mongo_count(m_objects, {"object_id":agent_object_id}) == 1
        obj = entry[0]
        # update agent object with info from parameters
        logger.info(msg=f'{function_name} service_id={obj["parameters"]["service_id"]}')
        m_objects.update_one({"object_id": agent_object_id},
                                 {"$set": {
                                     "service_host_url": _get_url(obj["parameters"]["service_id"]),
                                     "file_host_url": _get_url(obj["parameters"]["service_id"], "file_url"),
                                     "agent_status": "started"
                                 }})
        provider_params = obj["parameters"]
        provider_params["file_type"] = file_type
        logger.info(msg=f"provider_params={json.dumps(provider_params)}")
        provider_headers = {
            'accept': 'application/json',
            'Content-Type': 'multipart/form-data',
        }
        if(provider_params["accession_id"] != None):
            logger.info(msg=f'{function_name} ({file_type}) posting to url={_get_url(obj["parameters"]["service_id"])}/submit')
            response = requests.post(f'{_get_url(obj["parameters"]["service_id"])}/submit', params=provider_params, headers=provider_headers)
        else:
            # post file data to provider
            logger.info(msg=f'{function_name} ({file_type}) host_url={_get_url(obj["parameters"]["service_id"])}')
            (agent_file_dir, agent_file_name) = os.path.split(agent_file_path)
            logger.info(msg=f"{function_name} ({file_type}) posting file {agent_file_name} from directory {agent_file_path}, type {file_type}")
            file_data = {'client_file': open(agent_file_path, 'rb')}
            files = {'client_file': (f'{agent_file_name}', open(agent_file_path, 'rb')) }
            logger.info(msg=f'{function_name} ({file_type}) posting to url={_get_url(obj["parameters"]["service_id"])}/submit')
            response = requests.post(f'{_get_url(obj["parameters"]["service_id"])}/submit', params=provider_params, files=files)
            # unlink tmp copy of the posted file
            logger.info(msg=f"{function_name} ({file_type}) provider request complete for this file, removing file {agent_file_path}")
            os.unlink(agent_file_path)


        '''
            params = {
              "submitter_id": obj["parameters"]["submitter_id"],
              "data_type": obj["parameters"]["data_type"],
              "file_type": file_type,
              "version": "1.0"
            }
        '''

        # if successful, update agent object with status and provider object id
        if response.status_code == 200:        
            provider_object = response.json()
            #logger.info(msg=f"{function_name} ({file_type}) response={json.dumps(provider_object, indent=4)}")
            obj["loaded_file_objects"][file_type] = {}
            logger.info(msg=f'{function_name} Setting loaded_file_objects for {file_type} on {agent_object_id}')
            obj["loaded_file_objects"][file_type]["object_id"] = provider_object["object_id"]
            obj["loaded_file_objects"][file_type]["service_host_url"] = _get_url(obj["parameters"]["service_id"])
            obj["loaded_file_objects"][file_type]["file_host_url"] = _get_url(obj["parameters"]["service_id"], "file_url")
            logger.info(msg=f'{function_name}({file_type}) {provider_object["object_id"]}; loaded_file_objects={obj["loaded_file_objects"]}')
            m_objects.update_one({"object_id": agent_object_id},
                                 {"$set": {
                                     "loaded_file_objects": obj["loaded_file_objects"],
                                 }})
        else:
            detail_str = f'status_code={response.status_code}, response={response.text}'
            logger.error(msg=f"{function_name} ({file_type}) ! {detail_str}")
            m_objects.update_one({"object_id": agent_object_id},
                                     {"$set": {
                                         "agent_status": "failed",
                                         "detail": f'{function_name}: {detail_str}'
                                     }})            

        # unlink directory after all files have been processed
        logger.info(msg=f"{function_name} ({file_type}) object {agent_object_id} successfully created.")
        try:
            # check if another thread made an update:
            entry = m_objects.find({"object_id":agent_object_id}, {"_id":0})
            assert _mongo_count(m_objects, {"object_id":agent_object_id}) == 1
            obj = entry[0]
            logger.info(msg=f'{function_name} len(obj["loaded_file_objects"]=({len(obj["loaded_file_objects"])}), ({file_type}) obj={obj}')
            if len(obj["loaded_file_objects"]) == obj["num_files_requested"]:
                logger.info(msg=f"{function_name} ({file_type}) Removing directory {agent_file_dir}")
                os.rmdir(agent_file_dir)
                m_objects.update_one({"object_id": agent_object_id},
                                     {"$set": {
                                         "agent_status": "finished"
                                     }})
                logger.info(msg=f"{function_name} ({file_type}) ({agent_object_id}) agent_status = finished")
        except Exception as e:
            logger.error(msg=f'{function_name} ({file_type}) ! Exception {type(e)} occurred while attempting to unlink file {agent_file_dir} for object {agent_object_id}, message=[{e}] ! traceback={traceback.format_exc()}')


    except Exception as e:
        detail_str += f"! Exception {type(e)} occurred while submitting object to service, message=[{e}] ! traceback={traceback.format_exc()}"
        logger.error(msg=f"{function_name} ({file_type}) ! status=failed, {detail_str}")
        try:
            detail_str = f'Exception {type(e)} occurred while submitting object to service, obj=({agent_object_id}), message=[{e}] ! traceback={traceback.format_exc()}'
            m_objects.update_one({"object_id": agent_object_id},
                                     {"$set": {
                                         "agent_status": "failed",
                                         "detail": f'{function_name}: {detail_str}'
                                     }})
        except:
            logger.error(msg=f'{function_name} ({file_type}) ! unable to update object to failed.')
        logger.error(msg=f'{function_name} ({file_type}) ! updated object {agent_object_id} to failed.')
            


@app.post("/objects/load", summary="load object metadata and data for analysis from an end user or a 3rd party server", tags=["Post","Service","Data Provider Service","Tool Service"])
async def post_object(parameters: ProviderParameters = Depends(ProviderParameters.as_form),
                      requested_object_id: Optional[str] =  Query(None, title="Request an object id, not guaranteed. mainly for testing"),
                      optional_file_archive: UploadFile = File(None),
                      optional_file_expression: UploadFile = File(None),
                      optional_file_properties: UploadFile = File(None)
                      ):
    '''
    warning: executing this repeatedly for the same service/object will create duplicates in the database
    example request_url: submitter_id=krobasky%40renci.org&data_type=dataset-geneExpression&version=1.0
    Service will be called repeatedly, once per file and once per accession_id, based on what is provided.
    Must provide either an accession_id or one of the three optional files to avoid a 500 error
    The ids for the submitted files map to fields under the agent metadata's field, "loaded_file_objects", as follows:
            "filetype_dataset_archive": optional_file_archive,
            "filetype_dataset_expression": optional_file_expression,
            "filetype_dataset_properties": optional_file_properties

    If specifying an accession_id, then also specify the filetype(s) to be retreived by providing non-empty strings for the upload files of the desired types. This functionality is temporary to accommodate a prototype and will be replaced later (see https://github.com/RENCI/fuse-agent/issues/5)
    '''
    logger.info(msg=f"[post_object] top")
    try:
        # xxx be nice and assert config["configured-services"][parameters.service_id] exists, something like this:
        assert (parameters.service_id in _get_services())
        client_file_dict = {
            "filetype_dataset_archive": optional_file_archive,
            "filetype_dataset_expression": optional_file_expression,
            "filetype_dataset_properties": optional_file_properties
        }
        num_files_requested=0
        for file_type in client_file_dict.keys():
            num_files_requested=num_files_requested+(client_file_dict[file_type] is not None)
        assert num_files_requested > 0 or parameters.accession_id != None

        logger.info("[post_object] record submitter ({parameters.submitter_id}), if not found create one")
        add_submitter_response = api_add_submitter(parameters.submitter_id)
        
        #####
        # xxx This code is common with /analyze, break it out:
        # Insert a new agent object
        logger.info(msg=f"[post_object] getting id")
        agent_object_id = _gen_object_id("agent", parameters.submitter_id, requested_object_id, mongo_objects)
        timeout_seconds = g_redis_default_timeout # read this from config.json for the service xxx
        logger.info(msg=f"[post_object] submitter={parameters.submitter_id}, to service_id={parameters.service_id}, requesting {num_files_requested} files, timeout_seconds={timeout_seconds}")
        # stream any file(s) onto the fuse-agent server, named on fuse-agent from the client file name
        # xxx replace this with a ProviderObject model instance
        agent_object = {
            "object_id": agent_object_id,
            "created_time": datetime.utcnow(),
            "parameters": parameters.dict(), # xxx?
            "agent_status": None,
            "detail": None,
            
            "service_host_url": None, # url to provider, derived from params xxx - set this in callback
            "file_host_url": None, # url to the results data server (dataset urls are in the parameters  xxx - set this in callback
            
            # "service_object_id": None, # id created on provider for this upload
            "loaded_file_objects": {}, # e.g., "filetype_dataset_expression": "upload_<email>_<remote_object_id>", ...
            "num_files_requested": num_files_requested
        }
        logger.info(msg=f"[post_object] inserting agent-side object for provider={agent_object}")
        _mongo_insert(mongo_objects, agent_object)
        logger.info(msg=f"[post_object] created provider object: object_id:{agent_object_id}, submitter_id:{parameters.submitter_id}")
        # END
        #####

        # unlink files and directory (which may be empty) when you get into the job
        agent_path = _file_path(agent_object_id)
        os.mkdir(agent_path)
        logger.info(msg=f"[post_object] upload file path = {agent_path}")

        if parameters.accession_id is not None:
            logger.info(msg=f"[post_object] calling service with accession id = {parameters.accession_id, parameters.apikey}")
            
        for file_type in client_file_dict.keys():
            logger.info(msg=f"[post_object] top of loop, file_type= {file_type}") # ok so far
            client_file_obj = client_file_dict[file_type]
            agent_file_path = ""
            if client_file_obj is not None:
                client_file_name = client_file_obj.filename
                logger.info(msg=f"[post_object] getting file = {client_file_name}, file_type= {file_type}") # ok so far
                if parameters.accession_id == None: # xxx this is kludgey, revist (see https://github.com/RENCI/fuse-agent/issues/5)
                    agent_file_path = os.path.join(agent_path, client_file_name)
                    with open(agent_file_path, 'wb') as out_file:
                        contents = client_file_obj.file.read()
                        out_file.write(contents)
                    import time
                    logger.info(msg=f"[post_object] sleep a sec to try and avoid racing conditions")
                    time.sleep(3)
                else:
                    logger.info(msg=f"[post_object] accession_id provided, so no files uploaded for file_type={file_type}")
                    
                ###########
                # xxx This code is common with /load, break it out:
                # enqueue the job
                job_id = str(uuid.uuid4())
                # xxx maybe add to loaded_file_objects a status code and job_id?
                logger.info(msg=f"[post_object] QUEUE: agent_object_id:{agent_object_id}, file_type:{file_type}, agent_file_path:{agent_file_path},job_id=:{job_id}") # ok so far
                g_queue.enqueue(_remote_submit_file,
                                args=(agent_object_id, file_type, agent_file_path),
                                timeout=timeout_seconds,
                                job_id=job_id,
                                result_ttl=-1)
                mongo_objects.update_one({"object_id": agent_object_id},
                                         {"$set": {
                                             "agent_status": "queued"
                                         }})
                # xxx is this the right place for this?
                p_worker = Process(target=_initWorker)
                p_worker.start()
                # xxx should this be p_worker.work()?
                # END
                #####
        
        return {
            "object_id": agent_object_id,
            "submitter_action_status": add_submitter_response["submitter_action_status"]
        }
    except Exception as e:
        detail_str = f'Exception {type(e)} occurred while loading object to service=[{parameters.service_id}],  message=[{e}] ! traceback={traceback.format_exc()}'
        try:
            mongo_objects.update_one({"object_id": agent_object_id},
                                     {"$set": {
                                         "agent_status": "failed",
                                         "detail": f'{detail_str}'
                                     }})
        except:
            logger.error(msg=f"[post_object] ! unable to change agent_object_id to agent_status=failed")
        logger.error(msg=f"[post_object] ! {detail_str}")
        raise HTTPException(status_code=500,
                            detail=f"! {detail_str}")
    
    

    
@app.get("/objects/search/{submitter_id}", summary="get all object_ids accessible for this submitter", tags=["Get","Submitter"])
async def get_objects(submitter_id: str = Query(default=None, description="unique identifier for the submitter (e.g., email)")):
    '''
    returns {'object_id': <object_id>},
    use /objects/{object_id} to get object status and other metadata
    '''
    try:
        ret = list(map(lambda a: a, mongo_objects.find({"parameters.submitter_id": submitter_id}, {"_id": 0, "object_id": 1})))
        logger.info(msg=f"[get_objects] ret:{ret}")
        return ret
    except Exception as e:
        raise HTTPException(status_code=500,
                            detail=f"! Exception {type(e)} occurred while retrieving object_ids for submitter=({submitter_id}), message=[{e}] ! traceback={traceback.format_exc()}")


# xxx is this unecessary? can we just get the url from the provider at time of submission, cache it, and ignore the drs?
'''
def _parse_drs(drs_uri):
    #example:
    #drs:///{g_host_name}:{g_host_port}/{g_network}/{g_container_name}:{g_container_port}/{object_id}
    # xxx fix this:
    from urlparse import urlparse
    parsed_uri = urlparse(drs_uri)
    (server_host, server_port) = '{uri.netloc}'.format(uri=parsed_uri).split(":")
    if server_port == "":
        server_port = 80
    (container_network, container_netloc, object_id)= '{uri.netloc}'.format(uri=parsed_uri).split("/")
    (container_name, container_port) = container_netloc.split(":")
    drs_dict= {
        "server_host": server_host,
        "server_port": server_port,
        "container_network": container_network,
        "container_name": container_name,
        "container_port": container_port,
        "object_id" object_id
        }
    
    return drs_dict
'''

from starlette.responses import StreamingResponse
@app.get("/objects/url/{object_id}/type/{file_type}", summary="given a fuse-agent object_id, look up the metadata, find the DRS URI, parse out the URL to the file and return that", tags=["Get","Service","Data Provider Service","Tool Service"])
async def get_url(object_id: str,
                  file_type: FileType):
    '''
    filetype is one of "filetype_dataset_archive", "filetype_dataset_expression", "filetype_dataset_properties", "filetype_results_cellularFunction",  or "filetype_results_PCATable"
    '''
    try:
        # xxx make the parameter a FileType enum instead of a string
        logger.info(msg=f"[get_url] find local object={object_id}")
        entry = mongo_objects.find({"object_id": object_id},{"_id": 0})
        assert _mongo_count(mongo_objects, {"object_id":object_id}) == 1
        obj = entry[0]
        logger.info(msg=f'[get_url] found local object, agent_status={obj["agent_status"]}')
        logger.info(msg=f'[get_url] obj={obj}')
        assert obj["agent_status"] == "finished"

        # if the object was created from within a docker container, the service_host_url is going to be the container name, which you NEVER WANT for an externally accessible URL.
        # but you do want it for service calls across the docker network
        # how about creating a "file_host_url" field and populate it in /submit, /analyze with the config file
        obj_url = f'{obj["file_host_url"]}/files/{obj["loaded_file_objects"][file_type]["object_id"]}'
        logger.info(msg=f"[get_url] built url = ={obj_url}")
        return {"object_id": object_id, "url": obj_url}
    except Exception as e:
        raise HTTPException(status_code=500,
                            detail=f"! Exception {type(e)} occurred while building url for ({object_id}), message=[{e}] ! traceback={traceback.format_exc()}")
        
def _agent_delete_object(agent_object_id:str):
    ''' 
    Deletes agent object off local store; does NOT follow metadata to delete remote object.
    WARNING: call _remote_delete_object first in order to avoid orphaning cached objects that have been requested remotely
    returns:
       - array [info, stderr, status]: status can be one of "started", "deleted", "exception"
    '''
    function_name = "[_agent_delete_object]"
    delete_status = "started"
    info_msg=""
    stderr_msg=""
    try:
        # Request delete on remote server
        # because this may run out-of-band, I think we might need a new mongodb connection?
        logger.warn(msg=f"{function_name} Deleting agent agent_object_id: {agent_object_id}")
        ret = mongo_objects.delete_one({"object_id": agent_object_id})
        #<class 'pymongo.results.DeleteResult'>
        delete_status = "deleted"
        if ret.acknowledged != True:
            delete_status = "failed"
            info_msg += f"! Object not found on agent.\n{info_msg}"
            logger.error(msg=f"{function_name} agent delete failed, ret.acknowledged ! = True")
        if ret.deleted_count != 1:
            # should never happen if index was created for this field
            delete_status = "failed"
            info_msg += f"! Wrong number of records deleted from agent ({ret.deleted_count}).\n{info_msg}"
            logger.error(msg=f"{function_name} delete failed, wrong number deleted, count[1]={ret.deleted_count}")

        info_msg = f"{info_msg} Deleted agent objects, count=({ret.deleted_count}), Acknowledged=({ret.acknowledged})."
    except Exception as e:
        logger.error(msg=f"{function_name} Exception {type(e)} occurred while deleting agent {agent_object_id} from database, message=[{e}]  ! traceback={traceback.format_exc()}")
        stderr_msg += f"! Exception {type(e)} occurred while deleting agent {agent_object_id} from database, message=[{e}] ! traceback={traceback.format_exc()}"
        info_msg = f"! Somthing went wrong with local database delete.\n{info_msg}"
        delete_status = "exception"
        
    try:
        logger.info(msg=f"{function_name} Deleting cached files from agent {agent_object_id} off file system")
        local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
        local_path = os.path.join(local_path, f"{agent_object_id}-data")
        logger.info(msg=f"{function_name} removing tree ({local_path})")
        shutil.rmtree(local_path,ignore_errors=True)
    except Exception as e:
        logger.warn(msg=f"{function_name} Exception {type(e)} occurred while deleting agent {agent_object_id} from filesystem")
        stderr_msg += f"? Exception {type(e)} occurred while deleting agent object from filesystem, message=[{e}] ! traceback={traceback.format_exc()}"

    return [ info_msg, stderr_msg, delete_status]

def _remote_delete_object(agent_object_id:str):
    ''' 
    Deletes follows agent object id metadata to find and delete remote object. Does NOT delete agent object.
    WARNING: call _agent_delete_object after this call to avoid dangling references.
    returns array [info, stderr, status]
    status can be one of "started", "deleted", "not found", "failed"
    '''
    function_name="[_remote_delete_object]"
    delete_status = "started"
    info_msg = ''
    stderr_msg=''
    try:
        # Request delete on remote server
        logger.info(msg=f"{function_name} connecting to {g_mongo_client_str} anew")
        '''
        # because this may run out-of-band, I think we might need a new mongodb connection?
        my_mongo_client = pymongo.MongoClient(g_mongo_client_str)
        my_mongo_db = my_mongo_client.test
        m_objects=my_mongo_db.objects
        # xxx close this connection?
        '''
        
        logger.info(msg=f"{function_name} looking up {agent_object_id}")
        entry = mongo_objects.find({"object_id":agent_object_id},{"_id":0})
        assert _mongo_count(mongo_objects, {"object_id":agent_object_id}) == 1
        obj = entry[0]

        num_deleted = 0
        for file_type in obj["loaded_file_objects"]:
            info_msg = f'{info_msg}Deleting object file_type={file_type}. '
            service_object_id = obj["loaded_file_objects"][file_type]["object_id"]
            #service_object_id = obj["service_object_id"] # set this early in case there's an exception
            host_url = _get_url(obj["parameters"]["service_id"])
            delete_url=f"{host_url}/delete/{service_object_id}"
            logger.info(msg=f"{function_name} delete remote object with {delete_url}")
            response = requests.delete(delete_url)
            assert response.status_code == 200 or response.status_code == 404
            if response.status_code == 404:
                info_msg = f"{info_msg}* Remote object not found when deleting {agent_object_id}. "
                logger.warn(msg=f"{function_name} * Remote object {service_object_id} not found with {delete_url} when deleting {agent_object_id}")
                delete_status = "not found"
                # xxx may want to add this to an audit report for admin or something
            else:
                num_deleted = num_deleted + 1

        if num_deleted == len(obj["loaded_file_objects"]):
            delete_status = "deleted"

    except Exception as e:
        logger.warn(msg=f"{function_name} Exception {type(e)} occurred while deleting {agent_object_id}")
        stderr_msg += f'? Exception {type(e)} occurred while deleting agent objec {agent_object_id} message=[{e}] ! traceback={traceback.format_exc()}'
        delete_status = "failed"

    ret_obj = [info_msg, stderr_msg, delete_status]        
    logger.info(msg=f"{function_name} returning {ret_obj}")
    return ret_obj


# xxx connect this to delete associated analyses if object is dataset?
@app.delete("/delete/{object_id}", summary="DANGER ZONE: Delete a downloaded object; this action is rarely justified.", tags=["Delete","Service","Data Provider Service","Tool Service"])
async def delete(object_id: str,
                 force: bool = False):
    '''
    Delete cached data from the remote provider, identified by the provided object_id.
    <br>**WARNING**: This will orphan associated analyses; only delete downloads if:
    - the data are redacted.
    - the system state needs to be reset, e.g., after testing.
    - the sytem state needs to be corrected, e.g., after a bugfix.

    <br>**Note**: If the object was changed on the data provider's server, the old copy should be versioned in order to keep an appropriate record of the input data for past dependent analyses.
    <br>**Note**: Object will be deleted from disk regardless of whether or not it was found in the database. This can be useful for manual correction of erroneous system states.
    <br>**Returns**: 
    - status = 'deleted' if object is found in the database and 1 object successfully deleted.
    - status = 'exception' if an exception is encountered while removing the object from the database or filesystem, regardless of whether or not the object was successfully deleted, see other returned fields for more information.
    - status = 'failed' if 0 or greater than 1 object is not found in database.
    '''
    function_name = "[delete]"
    
    delete_status="started"
    # concatenate these as you go so they can be reported out in the event of an exception
    info=""
    stderr=""
    try:
        logger.info(msg=f"{function_name} deleting {object_id}")

        # delete object off provider
        remote_status=""
        try:
            logger.info(msg=f"{function_name} deleting {object_id} off remote service")
            # xxx may want to enqueue, but for now just call it directly
            # concatenate result to info, stderr
            [info, stderr, remote_status] = [ orig + " " + new for orig, new in zip([info, stderr, ''], _remote_delete_object(object_id)) ]
            remote_status = remote_status.lstrip()
        except Exception as e:
            logger.warn(msg=f"{function_name} ! exception while attempting to delete {object_id} off remote service")
            stderr += f'? Exception {type(e)} occurred while deleting provider object {object_id} message=[{e}] ! traceback={traceback.format_exc()}'

        # delete local object
        agent_status = ""
        try:
            if (remote_status == "deleted" or remote_status == "not found" or force):
                logger.info(msg=f"{function_name} deleting agent object={object_id}")
                # concatenate result to info, stderr
                [info, stderr, agent_status] = [ orig + " " + new for orig, new in zip([info, stderr, ''], _agent_delete_object(object_id)) ]
                delete_status = agent_status.lstrip()
            else:
                delete_status = remote_status
                logger.info(msg=f"{function_name} problem deleting object={object_id} on provider, status={remote_status}, info={info}, stderr={stderr}")
        except Exception as e:
            logger.error(msg=f"{function_name} ! exception while attempting to delete {object_id} off local service")
            stderr += f'? Exception {type(e)} occurred while deleting agent object {object_id} message=[{e}] ! traceback={traceback.format_exc()}'
            delete_status = "failed"

        ret= {
            "status": delete_status,
            "info": info,
            "stderr": stderr
        }
        logger.info(msg=f"{function_name} returning=({ret})")
        
        assert delete_status == "deleted"
        return ret
    
    except Exception as e:
        detail_str = f'! Message=[{info}] Error while deleting ({object_id}), status=[{delete_status}] stderr=[{stderr}]'
        logger.error(msg=f"{function_name} Exception {type(e)} occurred while deleting agent {object_id} from filesystem. detail_str={detail_str}")
        raise HTTPException(status_code=404,
                            detail=detail_str)

    
# xxx is this necessary? maybe just return status instead?
@app.get("/objects/{object_id}", summary="get metadata for the object", tags=["Get","Service","Data Provider Service","Tool Service"])
async def get_object(object_id: str = Query(default=None, description="unique identifier on agent to retrieve previously loaded object")):
    '''
    gets object's status and remote metadata
    Includes status and links to the input dataset, parameters, and dataset results if this object was created by a tool service
    '''
    try:
        # xxx retrieve metadata for each file type listed in loaded_file_objects
        logger.info(msg=f"[get_object] Finding metadata for object {object_id}")
        entry = mongo_objects.find({"object_id": object_id}, {"_id": 0})
        assert _mongo_count(mongo_objects, {"object_id":object_id}) == 1        
        obj = entry[0]
        logger.info(msg=f'[get_object] found local object, agent_status={obj["agent_status"]}')
        service_obj_metadata = None
        new_obj={}
        new_obj["agent"]= obj
        new_obj["provider"] = {}
        if obj["agent_status"] == "finished":
            logger.info(msg=f'[get_object] obj=({obj})')
            for file_type in obj["loaded_file_objects"]:
                logger.info(msg=f'[get_object] ***********file_type =({file_type})')
                logger.info(msg=f'[get_object] ***********obj =({obj})')
                service_object_id = obj["loaded_file_objects"][file_type]["object_id"]
                service_host_url = obj["loaded_file_objects"][file_type]["service_host_url"]
                logger.info(msg=f'[get_object] ({file_type}) REQUEST: {service_host_url}/objects/{service_object_id}')
                response = requests.get(f'{service_host_url}/objects/{service_object_id}')
                service_obj_metadata = response.json()
                logger.info(msg=f'[get_object] ({file_type}) METADATA={service_obj_metadata}')
                new_obj["provider"][file_type] = service_obj_metadata # xxx need to fill this in during queue

        return new_obj

    except Exception as e:
        raise HTTPException(status_code=404,
                            detail=f"! Exception {type(e)} occurred while retrieving metadata for ({object_id}), message=[{e}] ! traceback={traceback.format_exc()}")
    
async def _remote_analyze_object(agent_object_id:str, parameters:ToolParameters):
    function_name = "[_remote_analyze_object]"
    try:
        # INIT #####################################################################################################################
        logger.info(msg=f"{function_name}  connecting to {g_mongo_client_str} anew; agent_object_id:{agent_object_id} ")
        # xxx take this out?:
        my_mongo_client = pymongo.MongoClient(g_mongo_client_str)
        my_mongo_db = my_mongo_client.test
        m_objects=my_mongo_db.objects
        detail_str = ""
        timeout_seconds = g_redis_default_timeout # xxx read this from config.json, what's reasonable here?
        # get agent object
        logger.info(msg=f"{function_name} looking up {agent_object_id}")
        entry = m_objects.find({"object_id":agent_object_id},{"_id":0})
        assert _mongo_count(m_objects, {"object_id":agent_object_id}) == 1
        obj = entry[0]
        required_in_file_types=_get_service_value(obj["parameters"]["service_id"], "inputDatasetType", "file_types")
        # update to initialize agent object
        m_objects.update_one({"object_id": agent_object_id},
                             {"$set": {
                                 "service_host_url": _get_url(obj["parameters"]["service_id"]),
                                 "file_host_url": _get_url(obj["parameters"]["results_provider_service_id"],"file_url","results-provider-services"),
                                 "num_files_requested": len(required_in_file_types),
                                 "agent_status": "started"
                             }})
        logger.info(msg=f'{function_name} agent results object updated with tool and results server urls')
        # 1. get the data object(s) requested in the parameters #####################################################################
        try:
            entry = m_objects.find({"object_id": obj["parameters"]["dataset"]})
            dataset_obj = entry[0]
            logger.info(msg=f'{function_name} found one; file_host_url = {dataset_obj["file_host_url"]}')
            assert _mongo_count(m_objects, {"object_id": obj["parameters"]["dataset"]}) == 1
            # assert that the requested dataset data_type and files matche tool's inputDatasetType parameters of the requested tool
            for file_type in required_in_file_types:
                assert file_type in dataset_obj["loaded_file_objects"]
        except Exception as e:
            raise logger.error(msg='! {function_name} Exception {type(e)} occurred while attempting to collate dataset urls for ({agent_object_id}), parameters=({obj["parameters"]}) message=[{e}] ! traceback={traceback.format_exc()}')

        logger.info(msg=f'{function_name} params={json.dumps(obj["parameters"])}')
        
        # 2. post the dataset to the analysis endpoint ##################################################################################
        headers = {
            'accept': _get_service_value(obj["parameters"]["service_id"], "outputResultsType", "mime_type")
        }
        # fill in the analyze endpoint parameters with dataset urls
        for file_type in required_in_file_types:
            # for each file_type, remove 'filetype_dataset_', append '_url' to get the relevant tool parameter name
            param_name = f'{file_type.removeprefix("filetype_dataset_")}_url'
            logger.info(msg=f'{function_name} param_name={param_name}')
            # xxx the provider host url is <localhost> for non-container users
            # xxx but it's <container-name> for containerized tools.
            # so if there's a 'container_url configured, use that, otherwise default to the 'file_url'
            container_url = _get_url(dataset_obj["parameters"]["service_id"], "container_url")
            if container_url != "" and container_url != None:
                file_url = f'{container_url}/files/{dataset_obj["loaded_file_objects"][file_type]["object_id"]}'
            else: 
                file_url = f'{dataset_obj["file_host_url"]}/files/{dataset_obj["loaded_file_objects"][file_type]["object_id"]}'
            logger.info(msg=f'{function_name} file_url={file_url}')
            obj["parameters"][param_name] = file_url
            
        logger.info(msg=f'{function_name} parameters={obj["parameters"]}')
        logger.info(msg=f'{function_name} submit_url={_get_url(obj["parameters"]["service_id"])}/submit')
        '''
        POST /submit?
        service_id=fuse-tool-pca&
        submitter_id=f%40b.com&
        number_of_components=3&
        dataset=my10&
        expression_url=http://localhost:8083/files/%7B%27object_id%27%3A+%27upload_f%40b.com_fd55b250-c766-4908-83e7-2789ba77e1f1%27%2C+
        %27service_host_url%27%3A+
        %27http%3A%2F%2Flocalhost%3A8083%27%2C+
        %27file_host_url%27%3A+%27
        http%3A%2F%2Flocalhost%3A8083%27%7D&
        results_provider_service_id=fuse-provider-upload

        broken
        POST /submit?service_id=fuse-tool-pca&submitter_id=f%40b.com&number_of_components=3&dataset=my10&expression_url=http%3A%2F%2Flocalhost%3A8083%2Ffiles%2Fupload_f%40b.com_fd55b250-c766-4908-83e7-2789ba77e1f1&results_provider_service_id=fuse-provider-upload 1
        HTTP/1.1" 404 Not Found

        url = http://localhost:8083/files/upload_f@b.com_fd55b250-c766-4908-83e7-2789ba77e1f1
try:
        curl -X 'POST' \
        -F 'expression_file=@./t/input/expression.csv;type=application/vnd.ms-excel' \
        -H 'Content-Type: multipart/form-data' \
        -H 'accept: application/json' \
        'http://localhost:8086/submit?submitter_id=test@email.com&number_of_components=2&expression_' \
        head -10

        works
        curl -X 'POST' \
        -F 'expression_file=@./t/input/expression.csv;type=application/vnd.ms-excel' \
        -H 'Content-Type: multipart/form-data' \
        -H 'accept: application/json' \
        'http://localhost:8086/submit?submitter_id=test@email.com&number_of_components=2' \
        head -10

                2> /dev/null | python -m json.tool | jq --sort-keys

        '''
        analysis_response = requests.post(f'{_get_url(obj["parameters"]["service_id"])}/submit',
                                          params=obj["parameters"], headers=headers)
        logger.info(msg=f'{function_name} analysis_response.status_code={analysis_response.status_code}')
        assert analysis_response.status_code == 200

        # 3. store the results object in the results service ##################################################################
        # save results to /tmp
        # xxx maybe replace this with: {'client_file': open(io.StringIO(str(response.content,'utf-8')), 'rb')}
        results = analysis_response.content
        ext = _get_service_value(obj["parameters"]["service_id"], "outputResultsType", "file_extension")
        results_file_name=f'{agent_object_id}.{ext}'
        results_file_path = f'/tmp/{results_file_name}'
        with open(results_file_path, 'wb') as s:
            s.write(results) # xxx make this a zipped file
        logger.info(msg=f'{function_name} wrote response to {results_file_path}')
        # create results upload args
        files = {'client_file': (f'results-{agent_object_id}.{ext}', open(results_file_path, 'rb')) } # xxx make this a zipped file
        headers = {
            'accept': _get_service_value(obj["parameters"]["service_id"], "outputResultsType", "mime_type")
        }
        # fill in the parameters for the results
        results_file_type = _get_service_value(obj["parameters"]["service_id"], "outputResultsType", "file_type")
        params = {
            "submitter_id": obj["parameters"]["submitter_id"],
            "data_type": _get_service_value(obj["parameters"]["service_id"], "outputResultsType", "data_type"),
            "file_type": results_file_type,
            "version": "1.0"
        }
        logger.info(msg=f'{function_name} params={params}')
        # build results upload url
        results_provider_host_url = _get_url(obj["parameters"]["results_provider_service_id"],"service_url","results-provider-services")
        logger.info(msg=f'{function_name} results provider host_url={results_provider_host_url}')
        # call upload provider
        store_response = requests.post(f"{results_provider_host_url}/submit", params=params, files=files)
        logger.info(msg=f'{function_name} object added to results server, status code=({store_response.status_code})')
        # read the respones
        store_obj = store_response.json()
        logger.info(msg=f'{function_name} response = ({store_obj})')
        assert store_response.status_code == 200
        # xxx if the results file is a zip, add 'loaded files' meta data about the files here
        # unlink the /tmp file(s)
        os.unlink(results_file_path)
        # update the agent object to include the results object id
        logger.info(msg=f'{function_name} object_id={store_obj["object_id"]}')
        loaded_file_objects = {
            results_file_type: {
                "object_id": store_obj["object_id"],
                "service_host_url": _get_url(obj["parameters"]["results_provider_service_id"]),
                "file_host_url": _get_url(obj["parameters"]["results_provider_service_id"], "file_url")
            }
        }
        logger.info(msg=f'{function_name} *********  loaded_file_objects = {loaded_file_objects}')
        m_objects.update_one({"object_id": agent_object_id},
                             {"$set": {
                                 "loaded_file_objects": loaded_file_objects,
                                 "agent_status": "finished" # maybe add detail? xxx
                             }})
        # xxx need to fix the /files endpoint to return url to the single resutls file
        logger.info(msg=f"{function_name} ****DONE.*****")
        return
    except Exception as e:
        detail_str += f"! Exception {type(e)} occurred while analyzing dataset, message=[{e}] ! traceback={traceback.format_exc()}"
        logger.error(msg=f"{function_name} ! status=failed, {detail_str}")
        try:
            m_objects.update_one({"object_id": agent_object_id},
                                     {"$set": {
                                         "agent_status": "failed",
                                         "detail": f'{function_name}: {detail_str}'
                                     }})
        except:
            logger.error(msg=f'{function_name} ({agent_object_id}) ! unable to update object to failed.')
        logger.error(msg=f'{function_name} ! updated object {agent_object_id} to failed.')

@app.post("/analyze", summary="submit an analysis", tags=["Post","Service","Tool Service"])
async def analyze(parameters: ToolParameters = Depends(ToolParameters.as_form),
                  requested_results_object_id: Optional[str] =  Query(None, title="Request an object id for the results, not guaranteed. mainly for testing")):
    '''
from slack:
When an analysis is submitted to an agent, the agent will:
Create a "results"-type object_id in it's database, containing status="started" and the link to where you can get the object when its finished
enqueue the tool request
return the object_id
2. When the analysis job comes up, the agent updates the status, calls the tool, waits for a result,  and persists the result
3. When the dashboard asks for the object, the agent returns the meta data
4. If the meta data shows status = finished, the dashboard uses the link in the meta data to retrieve the results. (edited) 
    '''
    function_name="[analyze]"
    try:
        detail_str = ""
        dataset_object_id = None
        requested_object_id = requested_results_object_id
        logger.info(f"{function_name} if record for this submitter ({parameters.submitter_id}) is not found, create one")
        add_submitter_response = api_add_submitter(parameters.submitter_id)
        # quick check that dataset exist in the db
        dataset_object_id = parameters.dataset
        logger.info(f"{function_name} checking that dataset {dataset_object_id} for analysis are legit")
        assert  _mongo_count(mongo_objects, {"object_id": dataset_object_id}) == 1
        
        # add submitter
        logger.info(msg=f"{function_name} getting id")
        agent_object_id = _gen_object_id("agent", parameters.submitter_id, requested_object_id, mongo_objects)
        timeout_seconds = g_redis_default_timeout # read this from config.json for the service xxx
        # xxx replace this with a AgentObject model instance;
        results_provider_host_url = ''
        agent_object = {
            "object_id": agent_object_id,
            "created_time": datetime.utcnow(),
            "parameters": parameters.dict(), # parameters used for creating the results xxx works?
            "agent_status": None, # status of the analyses
            "detail": None, # any error messages
            
            "service_host_url": None, # url to tool, derived from params
            "file_host_url": None, # url to the results data server (dataset urls are in the parameters
            "loaded_file_objects": {}, # e.g., "filetype_results_PCATable": "..."
            "num_files_requested": 0
        }
        logger.info(msg=f"{function_name} inserting agent-side agent_object={agent_object}")
        _mongo_insert(mongo_objects, agent_object)
        logger.info(msg=f"{function_name} created agent object: object_id:{agent_object_id}, submitter_id:{parameters.submitter_id}")
        
        # enqueue the job
        job_id = str(uuid.uuid4())
        logger.info(msg=f"{function_name} submitter={parameters.submitter_id}, to service_id={parameters.service_id}, timeout_seconds={timeout_seconds}, job_id={job_id}")
        g_queue.enqueue(_remote_analyze_object,
                        args=(agent_object_id, parameters),
                        timeout=timeout_seconds,
                        job_id=job_id,
                        result_ttl=-1)
        logger.info(msg=f"{function_name} Updating status")
        mongo_objects.update_one({"object_id": agent_object_id},
                                 {"$set": {
                                     "agent_status": "queued"
                                 }})
        # xxx is this the right place for this?
        logger.info(msg=f"{function_name} Starting workers")
        p_worker = Process(target=_initWorker)
        p_worker.start()
        # xxx should this be p_worker.work()?
        
        return {
            "object_id": agent_object_id,
            "submitter_action_status": add_submitter_response["submitter_action_status"]
        }

    except Exception as e:
        raise HTTPException(status_code=404,
                            detail=f"{function_name} ! (dataset_object_id={dataset_object_id}) Exception {type(e)} occurred while running submit, message=[{e}] ! traceback={traceback.format_exc()}")
        

    
if __name__=='__main__':
        uvicorn.run("main:app", host='0.0.0.0', port=int(os.getenv("HOST_PORT")), reload=True )
