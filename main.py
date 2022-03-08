from datetime import datetime, timedelta
import dateutil.parser
import inspect
import os

from fastapi import FastAPI, File, UploadFile, Form, Depends, HTTPException, Query
from fastapi.logger import logger
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr, AnyUrl, Field, HttpUrl
from email_validator import validate_email, EmailNotValidError
from typing import Type, Optional, List, Union, Dict
from enum import Enum
from starlette.responses import StreamingResponse

import requests
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
    parameters: Dict = None
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
mongo_objects=mongo_db.objects

import pathlib
import json

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
                            detail=f"! Exception {type(e)} occurred while retrieving input schema for service submit, message=[{e}] \n! traceback=\n{traceback.format_exc()}\n")

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

@app.post("/submitters/add", summary="Create a record for a new submitter")
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
        

@app.get("/submitters/search", summary="Return a list of known submitters")
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

@app.get("/submitters/{submitter_id}", summary="Return metadata associated with submitter")
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

from multiprocessing import Process
from redis import Redis
from rq import Queue, Worker
from rq.job import Job
from rq.decorators import job
g_redis_default_timeout = 3600
g_redis_connection = Redis(host='agent-redis', port=6379, db=0) 
g_queue = Queue(connection=g_redis_connection, is_async=True, default_timeout=g_redis_default_timeout)
def _initWorker():
    worker = Worker(g_queue, connection=g_redis_connection)
    worker.work()

# xxx  does this ever get called?
# @job('low', connection=g_redis_connection, timeout=g_redis_default_timeout)
def _remote_submit_object(service_id, object_id, parameters):
    try:
        logger.info(msg=f"[_remote_submit_object] top of job")
        timeout_seconds = g_redis_default_timeout # xxx read this from config.json, what's reasonable here?
        import requests
        service_object_id = None # set this early in case there's an exception
        host_url = _get_url(service_id)
        mongo_objects.update_one({"object_id": object_id},
                                 {"$set": {
                                     "service_host_url": host_url,
                                     "agent_status": "started"
                                 }})
        logger.info(msg=f"[_remote_submit_object] host_url={host_url}")
        submit_url=f"{host_url}/submit"
        logger.info(msg=f"[_remote_submit_object] posting to url={submit_url}")
        response = requests.post(submit_url, data = parameters, timeout=timeout_seconds)
        json_obj = response.json()
        logger.info(msg=f"[_remote_submit_object] response={json.dumps(json_obj, indent=4)}")
        # xxx create unique local object_id and map it to the host url and object id for this service for retrieving later
        service_object_id = json_obj["object_id"]
        mongo_objects.update_one({"object_id": object_id},
                                 {"$set": {
                                     "service_object_id": service_object_id,
                                     "agent_status": "finished"
                                 }})
    except Exception as e:
        try:
            detail_str=f'Exception {type(e)} occurred while submitting object to service, obj=({object_id}), service_object_id=({service_object_id}) message=[{e}] \n! traceback=\n{traceback.format_exc()}\n'
            mongo_objects.update_one({"object_id": object_id},
                                     {"$set": {
                                         "service_object_id": service_object_id,
                                         "agent_status": "failed",
                                         "detail": f'[_remote_submit_object]: {detail_str}'
                                     }})
        except:
            logger.error(msg=f'[_remote_submit_object] ! unable to update object to failed.')
        
@app.post("/objects/load", summary="load object metadata and data from an end user or a 3rd party server")
async def post_object(submitter_id: Optional[str] = Query(default=None, description="unique identifier for the submitter (e.g., email)"),
                      service_id: str = Query(default=None, description="unique identifier for the submitter (e.g., email)"),
                      request_url: str = Query(default=None, description="string form of json parameters to pass to service_id, use /services/schema to retrieve required parameters"),
                      client_file: Optional[UploadFile] = File(...)
                      ):
    '''
    warning: executing this repeatedly for the same service/object will create duplicates in the database
    example request_url: submitter_id=krobasky%40renci.org&data_type=dataset-geneExpression&version=1.0
    '''
    try:
        import uuid
        local_object_id = str(uuid.uuid4())
        timeout_seconds = g_redis_default_timeout # read this from config.json for the service xxx
        logger.info(msg=f"[post_object] submitter={submitter_id}, posting request_url={request_url} to service_id={service_id}, timeout_seconds={timeout_seconds}")
        job_id = str(uuid.uuid4())
        # xxx replace this with a ProviderObject model instance
        provider_object = {"object_id": local_object_id,
                           "submitter_id": submitter_id,
                           "service_object_id": None,
                           "service_host_url": 1,
                           "job_id": job_id,
                           "agent_status": None,
                           "detail": None,
                           }
        # mongo_objects.insert(provider_object.dict())
        mongo_objects.insert(provider_object)
        logger.info(msg=f"[post_object] created provider object: object_id:{local_object_id}, submitter_id:{submitter_id}, job_id:{job_id}")
        # stream the file locally, unhook it when you get into the job
        g_queue.enqueue(_remote_submit_object,
                        args=(service_id, local_object_id, request_url),
                        timeout=timeout_seconds,
                        job_id=job_id,
                        result_ttl=-1)
        # xxx is this the right place for this?
        p_worker = Process(target=_initWorker)
        p_worker.start()
        # xxx should this be p_worker.work()?
        mongo_objects.update_one({"object_id": local_object_id},
                                 {"$set": {
                                     "agent_status": "queued"
                                 }})
        
        return {"object_id",local_object_id}
    except Exception as e:
        # xxx log error in database
        detail_str = f'Exception {type(e)} occurred while loading object to service=[{service_id}], \n request_url=[{request_url}] \n message=[{e}] \n! traceback=\n{traceback.format_exc()}\n'
        try:
            mongo_objects.update_one({"object_id": local_object_id},
                                     {"$set": {
                                         "agent_status": "failed",
                                         "detail": f'{detail_str}'
                                     }})
        except:
            logger.error(msg=f"[post_object] ! unable to change local_object_id to agent_status=failed")
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
                            detail="! Exception {type(e)} occurred while retrieving object_ids for submitter=({submitter_id}), message=[{e}] \n! traceback=\n{traceback.format_exc()}\n")


# xxx is this necessary? maybe just return status instead?
@app.get("/objects/{object_id}", summary="get metadata for the object")
async def get_object(object_id: str = Query(default=None, description="unique identifier on agent to retrieve previously loaded object")):
    '''
    gets object's status and remote metadata
    Includes status and links to the input dataset, parameters, and dataset results if this object was created by a tool service
    '''
    try:
        entry = mongo_objects.find({"object_id": object_id}, {"_id": 0,
                                                            "submitter_id": 1,
                                                            "service_object_id": 1,
                                                            "job_id": 1,
                                                            "agent_status": 1,
                                                            "detail": 1,
                                                            "service_host_url": 1,
                                                            "object_id": 1})
        assert entry.count() == 1
        obj = entry[0]
        if not "agent_status" in obj.keys():
            if "status" in obj.keys():
                obj["agent_status"] = obj["status"]
            else:
                obj["agent_status"] = "unknown"
            
        logger.info(msg=f'[get_object] found local object, agent_status={obj["agent_status"]}')
        
        if obj["agent_status"] == "finished":
            response = requests.get(f'{obj["service_host_url"]}/objects/{obj["service_object_id"]}')
            service_obj_metadata = response.json()
            service_obj_metadata["agent_status"] = obj["agent_status"]
            return service_obj_metadata
        else:
            return {"agent_status": obj["agent_status"],
                    "detail": obj["detail"],
                    }

    except Exception as e:
        raise HTTPException(status_code=500,
                            detail=f"! Exception {type(e)} occurred while retrieving metadata for ({object_id}), message=[{e}] \n! traceback=\n{traceback.format_exc()}\n")
    

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
        assert entry.count() == 1
        obj = entry.next()
        logger.info(msg=f'[get_url] found local object, agent_status={obj["agent_status"]}')
        assert obj["agent_status"] == "finished"
        obj_url = f'{obj["host_url"]}/files/{obj["service_object_id"]}'
        logger.info(msg=f"[get_url] built url = ={obj_url}")
        return obj_url
    except Exception as e:
        raise HTTPException(status_code=500,
                            detail=f"! Exception {type(e)} occurred while building url for ({object_id}), message=[{e}] \n! traceback=\n{traceback.format_exc()}\n")
        
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
        logger.info(msg=f"[get_url] returning url={file_url}\n")
        return file_url

    except Exception as e:
        raise HTTPException(status_code=404,
                            detail="! Exception {type(e)} occurred while retrieving for ({drs}), server({url}) message=[{e}] \n! traceback=\n{traceback.format_exc()}\n")
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


