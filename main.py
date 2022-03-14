import uvicorn
from datetime import datetime, timedelta
import dateutil.parser
import inspect
import os
import shutil

from fastapi import FastAPI, File, UploadFile, Form, Depends, HTTPException, Query
from fastapi.logger import logger
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr, AnyUrl, Field, HttpUrl
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
mongo_client_str = os.getenv("MONGO_CLIENT")
logger.info(msg=f"[MAIN] connecting to {mongo_client_str}")
mongo_client = pymongo.MongoClient(mongo_client_str)

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
    config_path = pathlib.Path(__file__).parent / "config.json"
    with open(config_path) as f:
        return json.load(f)

def _get_services(prefix = ""):
    assert prefix == "fuse-provider-" or prefix == "fuse-tool-" or prefix == ""
    config = _read_config()
    return list(filter(lambda x: x.startswith(prefix), list(config["configuredServices"])))
    
def _get_url(service_id: str):
    config = _read_config()
    service_host_name = config["configuredServices"][service_id]["host_name"]
    if service_host_name == os.getenv("HOST_NAME") or service_host_name == 'localhost':
        # co-located service, use container name and network instead:
        assert config["configuredServices"][service_id]["container-network"] == os.getenv("CONTAINER_NETWORK")
        url = config["configuredServices"][service_id]["container_URL"]
    else: 
        url = config["configuredServices"][service_id][f"http://{service_host_name}:{service_host_port}"]
    return url    

def _submitter_object_id(submitter_id):
    return "agent_" + submitter_id 

# xxx get with David to find out what else this should return in the json
@app.get("/services/providers", summary="Returns a list of the configured data providers")
async def providers():
    return _get_services("fuse-provider-")

@app.get("/services/tools", summary="Returns a list of the configured data tools")
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

                

# xxx add this to systems tests after all subsystems are integrated
@app.get("/services/schema/{service_id}", summary="returns the schema for the submit parameters required by the given service")
async def get_submit_parameters(service_id: str = Query(default="fuse-provider-upload", describe="loop through /providers or /tools to retrieve the submit parameters for each, providing the dashboard with everything it needs to render forms and solicit all the necessary information from the end user in order to load in datasets and/or run analyses")):
    try: 
        response = requests.get(f"{_get_url(service_id)}/openapi.json")
        json_obj = response.json()
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

@app.get("/services", summary="Returns a list of all configured services")
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
    if num_matches != 0:
        raise Exception(f"Submitter already added as: {object_id}, entries found = {num_matches}")
        
    submitter_object = Submitter(
        object_id = object_id,
        submitter_id = submitter_id,
        created_time = datetime.utcnow(),
        status = SubmitterStatus.approved)
    
    logger.info(msg=f"[api_add_submitter] submitter_object={submitter_object}")
    _mongo_insert(mongo_submitters, submitter_object.dict())
    logger.info(msg="[api_add_submitter] submitter added.")

    ret_val = {"submitter_id": submitter_id}
    logger.info(msg=f"[api_add_submitter] returning: {ret_val}")
    return ret_val
    
    
@app.post("/submitters/add", summary="Create a record for a new submitter")
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

@app.get("/submitters/search", summary="Return a list of known submitters")
async def get_submitters(within_minutes: Optional[int] = Query(default=None, description="find submitters created within the number of specified minutes from now")):
    '''
    return list of submitters
    '''
    try:
        return api_get_submitters(within_minutes)
    except Exception as e:
        raise HTTPException(status_code=404,
                            detail=f"! Exception {type(e)} occurred while searching submitters, message=[{e}] ! traceback={traceback.format_exc()}")
    
@app.delete("/submitters/delete/{submitter_id}", summary="Remove a submitter record")
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
    
@app.get("/submitters/{submitter_id}", summary="Return metadata associated with submitter")
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
    local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    return os.path.join(local_path, f"{object_id}-data")
          
@as_form
class ProviderParameters(BaseModel):
    service_id: str =        Field(...,
                                   title="Provider service id",
                                   description="id of service used to upload this object")
    submitter_id: EmailStr = Field(...,
                                   title="email",
                                   description="unique submitter id (email)")
    data_type: Optional[str] = Field(None, title="Data type of this object",
                                     description="the type of data; options are: dataset-geneExpression, results-pca, results-cellularFunction. Not all types are supported by all providers")
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
        # because this runs out-of-band, or maybe the async is doing it, I think we might need a new mongodb connection?
        logger.info(msg=f"[_remote_submit_file] connecting to {mongo_client_str} anew")
        my_mongo_client = pymongo.MongoClient(mongo_client_str)
        my_mongo_db = my_mongo_client.test
        m_objects=my_mongo_db.objects
        
        
        detail_str = ""
        timeout_seconds = g_redis_default_timeout # xxx read this from config.json, what's reasonable here?
        service_object_id = None # set this early in case there's an exception
        logger.info(msg=f"[_remote_submit_file] looking up {agent_object_id}")
        entry = m_objects.find({"object_id":agent_object_id},{"_id":0})
        obj = entry[0]
        assert _mongo_count(m_objects, {"object_id":agent_object_id}) == 1
        host_url = _get_url(obj["parameters"]["service_id"])
        m_objects.update_one({"object_id": agent_object_id},
                                 {"$set": {
                                     "service_host_url": host_url,
                                     "agent_status": "started"
                                 }})
        logger.info(msg=f"[_remote_submit_file] host_url={host_url}")
        submit_url=f"{host_url}/submit"
        logger.info(msg=f"[_remote_submit_file] posting to url={submit_url}")
        (agent_file_dir, agent_file_name) = os.path.split(agent_file_path)
        logger.info(msg=f"[_remote_submit_file] posting file {agent_file_name} from directory {agent_file_path}, type {file_type}")

        # xxx use this again for submitting an accession_id/apikey
        file_data = {'client_file': open(agent_file_path, 'rb')}
        headers = {
            'accept': 'application/json',
            'Content-Type': 'multipart/form-data',
        }
        # "data_type": file_type, # xxx 
        params = {
            "submitter_id": obj["parameters"]["submitter_id"],
            "data_type": "dataset-geneExpression", # xxx fix this!!
            "version": "1.0"
        }
        logger.info(msg=f"params={json.dumps(params)}")
        files = {'client_file': (f'{agent_file_name}', open(agent_file_path, 'rb')) }
        response = requests.post(submit_url, params=params, files=files)

        #response = requests.post('http://localhost:8083/submit?submitter_id=krobasky%40gmail.com&data_type=dataset-geneExpression&version=1.0', headers=headers, files=files)
        #response = requests.post(submit_url,
        #                        data = request_obj,
        #                       timeout=timeout_seconds,
        #                      files=file_data)
        
        logger.info(msg=f"[_remote_submit_file] removing file {agent_file_path}")
        os.unlink(agent_file_path)

        if response.status_code == 200:        
            json_obj = response.json()
            #logger.info(msg=f"[_remote_submit_file] response={json.dumps(json_obj, indent=4)}")
            service_object_id = json_obj["object_id"]
            logger.info(msg=f"[_remote_submit_file] service_object_id={service_object_id}")
            # xxx map the returned service_object_id back onto the agent_object_id but updatin that object
            m_objects.update_one({"object_id": agent_object_id},
                                 {"$set": {
                                     "service_object_id": service_object_id,
                                     "agent_status": "finished"
                                 }})
            ''' xxx ??? figure out how to handle a zipfile now
            loaded_file_objects = obj.file_objects.append({file_type: service_object_id})
            m_objects.update_one({"object_id": agent_object_id},
                                     {"$set": {
                                         "loaded_file_objects": loaded_file_objects,
                                         "agent_status": _set_agent_status(obj["parameters"]["accession_id"], obj["parameters"]["service_object_id"],
                                                                           obj["parameters"]["num_files_requested"],len(loaded_file_objects))
                                      }})
            '''
        else:
            detail_str = f'status_code={response.status_code}, response={response.text}'
            logger.error(msg=f"[_remote_submit_file] ! {detail_str}")
            m_objects.update_one({"object_id": agent_object_id},
                                     {"$set": {
                                         "agent_status": "failed",
                                         "detail": f'[_remote_submit_file]: {detail_str}'
                                     }})            

        # unlink directory after all files have been processed
        logger.info(msg=f"[_remote_submit_file] object {agent_object_id} successfully created, removing director {agent_file_dir}")
        try: 
            os.rmdir(agent_file_dir)
        except Exception as e:
            logger.error(msg=f'[_remote_submit_file] ! Exception {type(e)} occurred while attempting to unlink file {agent_file_dir} for object {agent_object_id}, message=[{e}] ! traceback={traceback.format_exc()}')


    except Exception as e:
        detail_str += f"! Exception {type(e)} occurred while submitting object to service, message=[{e}] ! traceback={traceback.format_exc()}"
        logger.error(msg=f"[_remote_submit_file] ! status=failed, {detail_str}")
        try:
            detail_str = f'Exception {type(e)} occurred while submitting object to service, obj=({agent_object_id}), service_object_id=({service_object_id}) message=[{e}] ! traceback={traceback.format_exc()}'
            m_objects.update_one({"object_id": agent_object_id},
                                     {"$set": {
                                         "service_object_id": service_object_id,
                                         "agent_status": "failed",
                                         "detail": f'[_remote_submit_file]: {detail_str}'
                                     }})
        except:
            logger.error(msg=f'[_remote_submit_file] ! unable to update object to failed.')
        logger.error(msg=f'[_remote_submit_file] ! updated object {agent_object_id} to failed.')
            


@app.post("/objects/load", summary="load object metadata and data for analysis from an end user or a 3rd party server")
async def post_object(parameters: ProviderParameters = Depends(ProviderParameters.as_form),
                      requested_object_id: Optional[str] =  Query(None, title="Request an object id, not guaranteed. mainly for testing"),
                      optional_file_archive: UploadFile = File(None),
                      optional_file_expressionMatrix: UploadFile = File(None),
                      optional_file_samplePropertiesMatrix: UploadFile = File(None)
                      ):
    '''
    warning: executing this repeatedly for the same service/object will create duplicates in the database
    example request_url: submitter_id=krobasky%40renci.org&data_type=dataset-geneExpression&version=1.0
    Service will be called repeatedly, once per file and once per accession_id, based on what is provided.
    Must provide either an accession_id or one of the three optional files to avoid a 500 error
    '''
    logger.info(msg=f"[post_object] top")
    try:
        logger.info("[post_object] adding submitter to submitters collection, as needed")
        try:
            submitter_object_id = api_get_submitter(parameters.submitter_id)
        except Exception as e:
            logger.info("[post_object] record for this submitter ({parameters.submitter_id}) not found, create one")
            api_add_submitter(parameters.submitter_id)
        
        client_file_dict = {
            "filetype-dataset-archive": optional_file_archive,
            "filetype-dataset-expression": optional_file_expressionMatrix,
            "filetype-dataset-properties": optional_file_samplePropertiesMatrix
        }
        num_files_requested=0
        for key in client_file_dict:
            num_files_requested=num_files_requested+(client_file_dict[key] is not None)
        assert num_files_requested > 0 or parameters.accession_id != None
        logger.info(msg=f"[post_object] getting id")
        agent_object_id = _gen_object_id("agent", parameters.submitter_id, requested_object_id, mongo_objects)
        
        timeout_seconds = g_redis_default_timeout # read this from config.json for the service xxx
        logger.info(msg=f"[post_object] submitter={parameters.submitter_id}, to service_id={parameters.service_id}, requesting {num_files_requested} files, timeout_seconds={timeout_seconds}")
        job_id = str(uuid.uuid4())
        # stream any file(s) onto the fuse-agent server, named on fuse-agent from the client file name
        # xxx replace this with a ProviderObject model instance
        provider_object = {
            "object_id": agent_object_id,
            "created_time": datetime.utcnow(),
            "parameters": parameters.dict(), # xxx?
            "job_id": job_id,
            "agent_status": None,
            "loaded_file_objects": [],
            "num_files_requested": num_files_requested,
            "detail": None,
            "service_object_id": None
        }
                               
        # xxx use this to get ProviderModel instance json: mongo_objects.insert(provider_object.dict())
        logger.info(msg=f"[post_object] inserting provider_object={provider_object}")
        _mongo_insert(mongo_objects, provider_object)
        logger.info(msg=f"[post_object] created provider object: object_id:{agent_object_id}, submitter_id:{parameters.submitter_id}, job_id:{job_id}")

        agent_file_paths=[]

        # unlink files and directory (which may be empty) when you get into the job
        agent_path = _file_path(agent_object_id)
        os.mkdir(agent_path)
        logger.info(msg=f"[post_object] upload file path = {agent_path}")

        if parameters.accession_id is not None:
            logger.info(msg=f"[post_object] calling service with accession id = {parameters.accession_id, parameters.apikey}")
            # xxx
            
        for key in client_file_dict:
            client_file_obj = client_file_dict[key]
            if client_file_obj is not None:
                file_type = key
                client_file_name = client_file_obj.filename
                logger.info(msg=f"[post_object] getting file = {client_file_name}, file_type= {file_type}")
                agent_file_path = os.path.join(agent_path, client_file_name)
                agent_file_paths.append(agent_file_path)
                with open(agent_file_path, 'wb') as out_file:
                    contents = client_file_obj.file.read()
                    out_file.write(contents)            
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
        
        return {"object_id": agent_object_id}
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
    
    

    
@app.get("/objects/search/{submitter_id}", summary="get all object_ids accessible for this submitter")
async def get_objects(submitter_id: str = Query(default=None, description="unique identifier for the submitter (e.g., email)")):
    '''
    returns {'object_id': <object_id>},
    use /objects/{object_id} to get object status and other metadata
    '''
    try:
        ret = list(map(lambda a: a, mongo_objects.find({"submitter_id": submitter_id}, {"_id": 0, "object_id": 1})))
        logger.info(msg=f"[get_objects] ret:{ret}")
        return ret
    except Exception as e:
        raise HTTPException(status_code=500,
                            detail="! Exception {type(e)} occurred while retrieving object_ids for submitter=({submitter_id}), message=[{e}] ! traceback={traceback.format_exc()}")


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
@app.get("/objects/url/{object_id}", summary="given a fuse-agent object_id, look up the metadata, find the DRS URI, parse out the URL to the file and return that")
async def get_url(object_id: str):
    '''
    '''
    try:
        logger.info(msg=f"[get_url] find local object={object_id}")
        entry = mongo_objects.find({"object_id": object_id},{"_id": 0, "service_object_id": 1, "service_host_url": 1, "agent_status": 1 })
        assert _mongo_count(mongo_objects, {"object_id":object_id}) == 1
        obj = entry.next()
        logger.info(msg=f'[get_url] found local object, agent_status={obj["agent_status"]}')
        assert obj["agent_status"] == "finished"
        obj_url = f'{obj["host_url"]}/files/{obj["service_object_id"]}'
        logger.info(msg=f"[get_url] built url = ={obj_url}")
        return obj_url
    except Exception as e:
        raise HTTPException(status_code=500,
                            detail=f"! Exception {type(e)} occurred while building url for ({object_id}), message=[{e}] ! traceback={traceback.format_exc()}")
        
    '''
    # xxx this is all unecessary?:

        #example:
        #drs:///{g_host_name}:{g_host_port}/{g_network}/{g_container_name}:{g_container_port}/{object_id}
        # xxx get the object_id DRS from the database
        drs_dict = _parse_drs(drs)
        if drs_dict["server_host"] == "localhost" or drs_dict["server_host"] == "0.0.0.0" or drs_dict["server_host"] == os.getenv('HOSTNAME'):
            # running locally, reference by container name
            assert os.getenv('CONTAINER_NETWORK')  == drs_dict["container_network"]
            host_name = f'http://drs_dict["container_name"]:drs_dict["container_port"]'
        else:
            host_name = f'https://drs_dict["server_host"]:drs_dict["server_port"]'
        logger.info(msg=f'[get_url] Retrieving {drs} at host={host_name}, object_id={drs_dict["object_id"]}')
        file_url = f"{host_name}/files/{object_id}"
        logger.info(msg=f"[get_url] returning url={file_url}")
        return file_url

    except Exception as e:
        raise HTTPException(status_code=404,
                            detail="! Exception {type(e)} occurred while retrieving for ({drs}), server({url}) message=[{e}] ! traceback={traceback.format_exc()}")
    '''


def _remote_delete_object(agent_object_id:str):
    delete_status = "started"
    ret_mongo=""
    ret_mongo_err=""
    try:
        ####
        # FIRST request delete on remote server

        # because this may run out-of-band, I think we might need a new mongodb connection?
        logger.info(msg=f"[_remote_delete_object] connecting to {mongo_client_str} anew")
        my_mongo_client = pymongo.MongoClient(mongo_client_str)
        my_mongo_db = my_mongo_client.test
        m_objects=my_mongo_db.objects
        
        logger.info(msg=f"[_remote_delete_object] looking up {agent_object_id}")
        entry = m_objects.find({"object_id":agent_object_id},{"_id":0})
        assert _mongo_count(m_objects, {"object_id":agent_object_id}) == 1
        obj = entry[0]
        service_object_id = obj["service_object_id"] # set this early in case there's an exception
        host_url = _get_url(obj["parameters"]["service_id"])
        delete_url=f"{host_url}/delete/{service_object_id}"
        logger.info(msg=f"[_remote_delete_object] delete remote object with {delete_url}")
        response = requests.delete(delete_url)
        #  ^^^^ everything to here works.
        assert response.status_code == 200 or response.status_code == 404
        
        ####
        # IF THAT WORKS delete object on agent:
        if response.status_code == 404:
            logger.warn(msg=f"[_remote_delete_object] Remote object {service_object_id} not found with {delete_url} when deleting {agent_object_id}")
            # xxx may want to add this to an audit report for admin or something
            
        logger.warn(msg=f"[_remote_delete_object] Deleting agent agent_object_id: {agent_object_id}")
        ret = m_objects.delete_one({"object_id": agent_object_id})
        #<class 'pymongo.results.DeleteResult'>
        delete_status = "deleted"
        if ret.acknowledged != True:
            delete_status = "failed"
            ret_mongo += "ret.acknoledged not True."
            info += f"Object not found on provider."
            logger.error(msg=f"[_remote_delete_object] agent delete failed, ret.acknowledged ! = True")
        if ret.deleted_count != 1:
            # should never happen if index was created for this field
            delete_status = "failed"
            ret_mongo += f"Wrong number of records deleted ({ret.deleted_count})."
            info += f"Wrong number of provider records deleted ({ret.deleted_count})."
            logger.error(msg=f"[_remote_delete_object] delete failed, wrong number deleted, count[1]={ret.deleted_count}")

        ret_mongo += f"Deleted agent objects, count=({ret.deleted_count}), Acknowledged=({ret.acknowledged})."
    except Exception as e:
        logger.error(msg=f"[_remote_delete_object] Exception {type(e)} occurred while deleting agent {agent_object_id} from database, message=[{e}]  ! traceback={traceback.format_exc()}")
        ret_mongo_err += f"! Exception {type(e)} occurred while deleting agent {agent_object_id} from database, message=[{e}] ! traceback={traceback.format_exc()}"
        info = f"Somthing went wrong with delete. {info}"
        delete_status = "exception"
        
    # If data are cached on a mounted filesystem, unlink that too if it's there
    logger.info(msg=f"[_remote_delete_object] Deleting agent {agent_object_id} from file system")
    ret_os=""
    ret_os_err=""
    try:
        # xxx this isn't working, remove file first?
        local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
        local_path = os.path.join(local_path, f"{agent_object_id}-data")
        logger.info(msg=f"[_remote_delete_object] removing tree ({local_path})")
        shutil.rmtree(local_path,ignore_errors=False)
    except Exception as e:
        logger.warn(msg=f"[_remote_delete_object] Exception {type(e)} occurred while deleting agent {agent_object_id} from filesystem")
        ret_os_err += f"? Exception {type(e)} occurred while deleting agent object from filesystem, message=[{e}] ! traceback={traceback.format_exc()}"

    return {
        "status": delete_status,
        "info": f"{ret_mongo} {ret_os}",
        "stderr": f"{ret_mongo_err} {ret_os_err}"
    }

    
# xxx connect this to delete associated analyses if object is dataset?
@app.delete("/delete/{object_id}", summary="DANGER ZONE: Delete a downloaded object; this action is rarely justified.")
async def delete(object_id: str):
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
    info=""
    delete_status=""
    stderr=""
    try:
        # may want to enqueue, but for now just call it directly
        logger.info(msg=f"[delete] deleting {object_id}")
        ret = _remote_delete_object(object_id)
        logger.info(msg=f"[delete] returned ({ret})")
        info=ret["info"]
        delete_status=ret["status"]
        stderr=ret["stderr"]
        assert delete_status == "deleted"
        return ret
    except Exception as e:
        detail_str = f'! Message=[{info}] Error while deleting ({object_id}), status=[{delete_status}] stderr=[{stderr}]'
        logger.error(msg=f"[delete] Exception {type(e)} occurred while deleting agent {object_id} from filesystem. detail_str={detail_str}")
        raise HTTPException(status_code=404,
                            detail=detail_str)

    
# xxx is this necessary? maybe just return status instead?
@app.get("/objects/{object_id}", summary="get metadata for the object")
async def get_object(object_id: str = Query(default=None, description="unique identifier on agent to retrieve previously loaded object")):
    '''
    gets object's status and remote metadata
    Includes status and links to the input dataset, parameters, and dataset results if this object was created by a tool service
    '''
    try:
        logger.info(msg=f"[get_object] Finding metadata for object {object_id}")
        entry = mongo_objects.find({"object_id": object_id}, {"_id": 0})
        assert _mongo_count(mongo_objects, {"object_id":object_id}) == 1        
        obj = entry[0]
        logger.info(msg=f'[get_object] found local object, agent_status={obj["agent_status"]}')
        service_obj_metadata = None
        if obj["agent_status"] == "finished":
            response = requests.get(f'{obj["service_host_url"]}/objects/{obj["service_object_id"]}')
            service_obj_metadata = response.json()
            logger.info(msg=f'[get_object] metadata={service_obj_metadata}')

        obj["provider"] = service_obj_metadata
        return obj

    except Exception as e:
        raise HTTPException(status_code=404,
                            detail=f"! Exception {type(e)} occurred while retrieving metadata for ({object_id}), message=[{e}] ! traceback={traceback.format_exc()}")
    

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

if __name__=='__main__':
        uvicorn.run("main:app", host='0.0.0.0', port=int(os.getenv("HOST_PORT")), reload=True )
