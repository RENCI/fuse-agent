import json
import logging
import os
import shutil
import time
import traceback
import uuid
from datetime import datetime, timedelta
from logging.config import dictConfig
from typing import Optional

import nest_asyncio
import pymongo
import requests
import uvicorn
from fastapi import FastAPI, File, UploadFile, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from redis import Redis
from rq import Queue

from fuse.models.Config import LogConfig
from fuse.models.Objects import ServiceIOType, ServiceIOField, SubmitterActionStatus, Submitter, SubmitterStatus, ToolParameters, ProviderParameters, FileType, config

nest_asyncio.apply()

dictConfig(LogConfig().dict())
logger = logging.getLogger("fuse-agent")

tags_metadata = [
    {"name": "Data Provider Service", "description": "Call out to 3rd party data provider services"},
    {"name": "Tool Service", "description": "Call out to 3rd party tool services (may query a data provider along the way)"},
    {"name": "Submitter", "description": "Manage users"},
    {"name": "Service", "description": "Tools and provider methods"},
    {"name": "Get", "description": "Query only: All the methods you can use with a 'get' http call"},
    {"name": "Post", "description": "Updata persistent data: All the methods you can use with a 'post' http call"},
    {"name": "Delete", "description": "WARNING: Only use these calls for redacting data, correcting an inconsistent database state, or removing test data."}
]

g_api_version = "0.0.1"

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
              })

origins = [
    f"http://{os.getenv('HOST_NAME')}:{os.getenv('HOST_PORT')}",
    f"http://{os.getenv('HOST_NAME')}",
    f"http://{os.getenv('CONTAINER_NAME')}:{os.getenv('CONTAINER_PORT')}"
    f"http://{os.getenv('CONTAINER_NAME')}"
    f"http://localhost:{os.getenv('HOST_PORT')}",
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

config_json = config()

g_mongo_client_str = os.getenv("MONGO_CLIENT")
logger.info(f"connecting to {g_mongo_client_str}")
mongo_client = pymongo.MongoClient(g_mongo_client_str)

mongo_db = mongo_client.test
mongo_db_version = mongo_db.command({'buildInfo': 1})['version']
mongo_db_major_version = mongo_client.server_info()["versionArray"][0]
mongo_db_minor_version = mongo_client.server_info()["versionArray"][1]
mongo_agent = mongo_db.agent
mongo_submitters = mongo_db.submitters
mongo_objects = mongo_db.objects

g_redis_default_timeout = os.getenv("REDIS_TIMEOUT")
g_redis_connection = Redis(host=os.getenv("REDIS_HOST"), port=os.getenv("REDIS_PORT"), db=0)
logger.info(f'redis host={os.getenv("REDIS_HOST")}:{os.getenv("REDIS_PORT")}')
g_queue = Queue(connection=g_redis_connection, is_async=True, default_timeout=g_redis_default_timeout)


# mongo migration functions to support running outside of container with more current instance
def _mongo_insert(coll, obj):
    if mongo_db_major_version < 4:
        logger.info(f"using collection.insert")
        coll.insert(obj)
    else:
        logger.info(f"using collection.insert_one")
        coll.insert_one(obj)


def _mongo_count(coll, obj):
    if mongo_db_major_version < 3 and mongo_db_minor_version < 7:
        logger.info(f"mongodb version = {mongo_db_version}, use deprecated entry count function")
        entry = coll.find(obj, {})
        num_matches = entry[0].count()
    else:
        logger.info(f"mongo_db version = {mongo_db_version}, use count_documents function")
        num_matches = coll.count_documents(obj)
    logger.info(f"found ({num_matches}) matches")
    return num_matches


# end mongo migration functions

def _get_services(prefix=""):
    assert prefix == "fuse-provider-" or prefix == "fuse-tool-" or prefix == ""
    return list(filter(lambda x: x.startswith(prefix), list(config_json["configured-services"])))


def _get_url(service_id: str, url_type: str = "service_url", host_type: str = "configured-services"):
    logger.info(f'service_id={service_id}, url_type={url_type}, host_type={host_type}')
    return config_json[host_type][service_id][url_type]


def _submitter_object_id(submitter_id):
    return "agent_" + submitter_id


####################
# service-info methods and cdm's
def _get_service_info(service_id):
    url = f'{config_json["configured-services"][service_id]["service_url"]}/service-info'
    logger.info(f"url: {url}")
    response = requests.get(url)
    return response.json()


def _get_service_value(service_id, iotype: ServiceIOType, field: ServiceIOField):
    assert service_id.startswith("fuse-tool-")
    service_info = _get_service_info(service_id)
    logger.info(f"service_info: {service_info}")
    types = set()
    for idx, entry in enumerate(service_info[iotype]):
        logger.info(f"idx: {idx}, entry: {entry}")
        types.add(entry[field])
    return types


# xxx get with David to find out what else this should return in the json
@app.get("/services/providers", summary="Returns a list of the configured data providers", tags=["Get", "Service", "Data Provider Service"])
async def providers():
    return _get_services("fuse-provider-")


@app.get("/services/tools", summary="Returns a list of the configured data tools", tags=["Get", "Service", "Tool Service"])
async def tools():
    return _get_services("fuse-tool-")


def _resolve_ref(ref, models):
    (refpath, model_name) = os.path.split(ref["$ref"])
    logger.info(f" referenced path={refpath}, model={model_name} ")
    _resolve_refs(models[model_name], models)
    return model_name


def _resolve_refs(doc, models):
    if type(doc) == dict:
        if "$ref" in doc:
            model_name = _resolve_ref(doc, models)
            doc[model_name] = models[model_name]
            del doc["$ref"]
            logger.info(f"STOP:resolved[name={model_name}, obj={doc[model_name]}]")
            return
        else:
            for k, v in doc.items():
                logger.info(f"resolving dict key:{k}, value:{v}")
                _resolve_refs(doc[k], models)
    elif type(doc) == list:
        for elem in doc:
            logger.info(f"resolving list element {elem}")
            _resolve_refs(elem, models)
    else:
        logger.info(f"STOP:doc type ({type(doc)}) for leaf doc={doc}")
        return


@app.get("/services/result_types/{service_id}", summary="types of results supported by this tool service")
async def get_tool_result_types(service_id: str = Query(default="fuse-tool-pca",
                                                        describe="loop through /tools to retrieve the results types for each, providing the dashboard with everything it needs to render forms and solicit all the necessary information from the end user in order to analyze results")):
    """
    so far, known values are:
    result-type-pcaTable
    result-type-cellfie
    """
    raise HTTPException(status_code=404, detail=f"! get_tool_result_types under construction")


# xxx add this to systems tests after all subsystems are integrated
@app.get("/services/schema/{service_id}", summary="returns the schema for the submit parameters required by the given service",
         tags=["Get", "Service", "Data Provider Service", "Tool Service"])
async def get_submit_parameters(service_id: str = Query(default="fuse-provider-upload",
                                                        describe="loop through /providers or /tools to retrieve the submit parameters for each, providing the dashboard with everything it needs to render forms and solicit all the necessary information from the end user in order to load in datasets and/or run analyses"),
                                version: str = Query(default="0.0.1", describe="version of the api to use, 0.0.1 by default")):
    try:
        host_url = _get_url(f'{service_id}')
        openapi_url = f'{host_url}/api/{version}/openapi.json'
        logger.info(f'openapi_url={openapi_url}')
        response = requests.get(f"{openapi_url}")
        json_obj = response.json()
        logger.info(f'response={json_obj}')
        params = json_obj['paths']['/submit']['post']['parameters']
        components = json_obj['components']['schemas']
        # look for any referenced data models and fill them in
        # this is helpful: https://swagger.io/docs/specification/using-ref/
        #                  https://stackoverflow.com/questions/60010686/how-do-i-access-the-ref-in-the-json-recursively-as-to-complete-the-whole-json
        # Recurses through dictionaries, lists, and nested references; doesn't handle referenced files
        logger.info(f"resolving submit params={params}")
        logger.info(f"components={components}")
        _resolve_refs(params, components)
        return params
    except Exception as e:
        raise HTTPException(status_code=500,
                            detail=f"! Exception {type(e)} occurred while retrieving input schema for service submit, message=[{e}] ! traceback={traceback.format_exc()}")


@app.get("/services", summary="Returns a list of all configured services", tags=["Get", "Service", "Data Provider Service", "Tool Service"])
async def all_services():
    """
    once you have the list of services, you can call each one separately to get the descriptoin of parameters to give to end-users;
    for example, this to get the full schema forthe parameters required for submitting an object to be loaded by a data provider:
    /services/schema/{service_id}
    Aside: this is done internally by requesting the openapi.json from the fastapi-enabled service, similar to:
    curl -X 'GET' 'fuse-provider-upload:8000/openapi.json
    """
    return _get_services("")


def api_add_submitter(submitter_id: str):
    object_id = _submitter_object_id(submitter_id)
    num_matches = _mongo_count(mongo_submitters, {"object_id": object_id})

    submitter_action_status = SubmitterActionStatus.unknown
    if num_matches == 1:
        submitter_action_status = SubmitterActionStatus.existed
    else:
        assert num_matches == 0
        submitter_object = Submitter(object_id=object_id, submitter_id=submitter_id, created_time=datetime.utcnow(), status=SubmitterStatus.approved)
        logger.info(f"submitter_object={submitter_object}")
        _mongo_insert(mongo_submitters, submitter_object.dict())
        logger.info("submitter added.")
        submitter_action_status = SubmitterActionStatus.created

    ret_val = {
        "submitter_id": submitter_id,
        "submitter_action_status": submitter_action_status
    }
    logger.info(f"returning: {ret_val}")
    return ret_val


@app.post("/submitters/add", summary="Create a record for a new submitter", tags=["Post", "Submitter"])
async def add_submitter(submitter_id: str = Query(default=None, description="unique identifier for the submitter (e.g., email)")):
    """
    Add a new submitter
    """
    try:
        return api_add_submitter(submitter_id)
    except Exception as e:
        logger.info(f"exception, ! Exception {type(e)} occurred while inserting submitter ({submitter_id}), message=[{e}] ! traceback={traceback.format_exc()}")
        raise HTTPException(status_code=404,
                            detail=f"! Exception {type(e)} occurred while inserting submitter ({submitter_id}), message=[{e}] ! traceback={traceback.format_exc()}")


def api_get_submitters(within_minutes: int = None):
    if within_minutes is not None:
        logger.info(f"get submitters created within the last {within_minutes} minutes.")
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
        logger.info("get all.")
        search_object = {}
    ret = list(map(lambda a: a, mongo_submitters.find(search_object, {"_id": 0, "submitter_id": 1})))
    logger.info(f"ret:{ret}")
    return ret


@app.get("/submitters/search", summary="Return a list of known submitters", tags=["Get", "Submitter"])
async def get_submitters(within_minutes: Optional[int] = Query(default=None, description="find submitters created within the number of specified minutes from now")):
    """
    return list of submitters
    """
    try:
        return api_get_submitters(within_minutes)
    except Exception as e:
        raise HTTPException(status_code=404,
                            detail=f"! Exception {type(e)} occurred while searching submitters, message=[{e}] ! traceback={traceback.format_exc()}")


@app.delete("/submitters/delete/{submitter_id}", summary="Remove a submitter record", tags=["Delete", "Submitter"])
async def delete_submitter(submitter_id: str = Query(default=None, description="unique identifier for the submitter (e.g., email)")):
    """
    deletes submitter and their datasets, analyses
    """
    delete_status = "done"
    ret_mongo = ""
    ret_mongo_err = ""
    try:
        logger.warning(f"Deleting submitter_id:{submitter_id}")
        ret = mongo_submitters.delete_one({"submitter_id": submitter_id})
        # <class 'pymongo.results.DeleteResult'>
        delete_status = "deleted"
        if ret.acknowledged is not True:
            delete_status = "failed"
            ret_mongo += "ret.acknoledged not True."
            logger.error("delete failed, ret.acknowledged ! = True")
        if ret.deleted_count != 1:
            # should never happen if index was created for this field
            delete_status = "failed"
            ret_mongo += f"Wrong number of records deleted ({ret.deleted_count})./n"
            logger.error(f"delete failed, wrong number deleted, count[1]={ret.deleted_count}")
        ## xxx
        # could check if there are any remaining; but this should instead be enforced by creating an index for this columnxs
        # could check ret.raw_result['n'] and ['ok'], but 'ok' seems to always be 1.0, and 'n' is the same as deleted_count
        ##
        ret_mongo += f"Deleted count=({ret.deleted_count}), Acknowledged=({ret.acknowledged})./n"
    except Exception as e:
        logger.error(f"Exception {type(e)} occurred while deleting {submitter_id} from database, message=[{e}]")
        ret_mongo_err += f"! Exception {type(e)} occurred while deleting {submitter_id}) from database, message=[{e}] ! traceback={traceback.format_exc()}"
        delete_status = "exception"

    ret = {
        "status": delete_status,
        "info": ret_mongo,
        "stderr": ret_mongo_err
    }
    logger.info(f"returning ({ret})")
    return ret


def api_get_submitter(submitter_id):
    """
     Expects exactly 1 match, throws exception otherwise
    """
    object_id = _submitter_object_id(submitter_id)
    entry = mongo_submitters.find({"object_id": object_id}, {"_id": 0})
    num_matches = _mongo_count(mongo_submitters, {"object_id": object_id})
    logger.info(f"found ({num_matches}) matches for object_id={object_id}")
    assert num_matches == 1
    ret_val = entry[0]
    logger.info(f"returning: {ret_val}")
    return ret_val


@app.get("/submitters/{submitter_id}", summary="Return metadata associated with submitter", tags=["Get", "Submitter"])
async def get_submitter(submitter_id: str = Query(default=None, description="unique identifier for the submitter (e.g., email)")):
    try:
        return api_get_submitter(submitter_id)
    except Exception as e:
        raise HTTPException(status_code=404,
                            detail=f"! Exception {type(e)} occurred while finding submitter ({submitter_id}), message=[{e}] ! traceback={traceback.format_exc()}")


def _gen_object_id(prefix, submitter_id, requested_object_id, coll):
    try:
        object_id = f"{prefix}_{submitter_id}_{uuid.uuid4()}"
        if requested_object_id is not None:
            logger.info(f"top prefex={prefix}, submitter={submitter_id}, requested:{requested_object_id}")
            entry = coll.find({"object_id": requested_object_id}, {"_id": 0, "object_id": 1})
            num_matches = _mongo_count(coll, {"object_id": requested_object_id})
            if num_matches >= 1:
                logger.info(f"found ({num_matches}) matches for requested object_id={requested_object_id}")
                return requested_object_id
        return object_id
    except Exception as e:
        logger.exception(f"? Exception {type(e)} occurred when using {requested_object_id}, using {object_id} instead. message=[{e}] ! traceback={traceback.format_exc()}")


def _set_agent_status(accession_id, service_object_id, num_files_requested, num_loaded_files):
    # status = finished if
    #      len(file_objects) = num_files_requested and
    #      if obj.parameters.accession_id is not None, then service_object_id is not None
    if num_files_requested == num_loaded_files and (accession_id is None or service_object_id is not None):
        return "finished"
    else:
        return "started"


# @job('low', connection=g_redis_connection, timeout=g_redis_default_timeout)
async def _remote_submit_file(agent_object_id: str, file_type: str, agent_file_path: str, job_id: str):
    try:
        # because this runs out-of-band, or maybe the async is doing it, I think we might need a new mongodb connection?
        logger.info(f"({file_type}) connecting to {g_mongo_client_str} anew; agent_object_id:{agent_object_id} file_type:{file_type}, agent_file_path:{agent_file_path} ")
        my_mongo_client = pymongo.MongoClient(g_mongo_client_str)
        my_mongo_db = my_mongo_client.test
        m_objects = my_mongo_db.objects
        detail_str = ""
        timeout_seconds = g_redis_default_timeout  # xxx read this from config.json, what's reasonable here?
        service_object_id = None  # set this early in case there's an exception
        # get the agent object for this job
        logger.info(f"({file_type}) looking up {agent_object_id}")
        entry = m_objects.find({"object_id": agent_object_id}, {"_id": 0})
        assert _mongo_count(m_objects, {"object_id": agent_object_id}) == 1
        obj = entry[0]
        # update agent object with info from parameters
        logger.info(f'service_id={obj["parameters"]["service_id"]}')
        m_objects.update_one({"object_id": agent_object_id},
                             {"$set": {
                                 "service_host_url": _get_url(obj["parameters"]["service_id"]),
                                 "file_host_url": _get_url(obj["parameters"]["service_id"], "file_url"),
                                 "agent_status": "started"
                             }})
        provider_params = obj["parameters"]
        provider_params["file_type"] = file_type
        provider_headers = {'accept': 'application/json'}
        endpoint = f'{_get_url(obj["parameters"]["service_id"])}/submit'
        logger.info(f"endpoint: {endpoint}, provider_headers: {provider_headers}, provider_params: {provider_params}")
        if provider_params["accession_id"] is not None:
            response = requests.post(f'{endpoint}', data=provider_params, headers=provider_headers, timeout=3600)
        else:
            (agent_file_dir, agent_file_name) = os.path.split(agent_file_path)
            logger.info(f"agent_file_dir: {agent_file_dir}), agent_file_name: {agent_file_name}")
            files = {'client_file': (f'{agent_file_name}', open(agent_file_path, 'rb'))}
            logger.info(f'({file_type}) posting to url={_get_url(obj["parameters"]["service_id"])}/submit')
            response = requests.post(f'{_get_url(obj["parameters"]["service_id"])}/submit', data=provider_params, files=files, timeout=3600)
            # unlink tmp copy of the posted file
            logger.info(f"({file_type}) provider request complete for this file, removing file {agent_file_path}")
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
            logger.info(f'PROVIDER RESPONSE=({file_type}) {provider_object}')
            # xxx warning: if two threads are updating the same loaded_file_objects json object at once, only one file_type will be set.
            logger.info(f'Setting loaded_file_objects for {file_type} on {agent_object_id}')
            loaded_file_object = {
                "object_id": provider_object["object_id"],
                "service_host_url": _get_url(obj["parameters"]["service_id"]),
                "file_host_url": _get_url(obj["parameters"]["service_id"], "file_url")
            }
            logger.info(f'({file_type}) {provider_object["object_id"]}; loaded_file_objects={obj["loaded_file_objects"]}')
            m_objects.update_one({"object_id": agent_object_id},
                                 {"$set": {
                                     f'loaded_file_objects.{file_type}': loaded_file_object
                                 }})
        else:
            detail_str = f'status_code={response.status_code}, response={response.text}'
            logger.error(f"({file_type}) ! {detail_str}")
            m_objects.update_one({"object_id": agent_object_id},
                                 {"$set": {
                                     "agent_status": "failed",
                                     "detail": f'_remote_submit_file: {detail_str}'
                                 }})

        logger.info(f"({file_type}) object {agent_object_id} successfully created.")
        try:
            # check if another thread made an update:
            entry = m_objects.find({"object_id": agent_object_id}, {"_id": 0})
            assert _mongo_count(m_objects, {"object_id": agent_object_id}) == 1
            obj = entry[0]
            logger.info(f'len(obj["loaded_file_objects"]=({len(obj["loaded_file_objects"])}), ({file_type}) obj={obj}')
            if len(obj["loaded_file_objects"]) == obj["num_files_requested"]:
                m_objects.update_one({"object_id": agent_object_id},
                                     {"$set": {"agent_status": "finished"}})
                logger.info(f"({file_type}) ({agent_object_id}) agent_status = finished")

                if provider_params["accession_id"] is None:
                    # unlink directory after all files have been processed
                    logger.info(f"({file_type}) Removing directory {agent_file_dir}")
                    os.rmdir(agent_file_dir)

        except Exception as e:
            logger.error(
                f'({file_type}) ! Exception {type(e)} occurred while attempting to unlink file {agent_file_dir} for object {agent_object_id}, message=[{e}] ! traceback={traceback.format_exc()}')

    except Exception as e:
        detail_str += f"! Exception {type(e)} occurred while submitting object to service, message=[{e}] ! traceback={traceback.format_exc()}"
        logger.error(f"({file_type}) ! status=failed, {detail_str}")
        try:
            detail_str = f'Exception {type(e)} occurred while submitting object to service, obj=({agent_object_id}), message=[{e}] ! traceback={traceback.format_exc()}'
            m_objects.update_one({"object_id": agent_object_id},
                                 {"$set": {"agent_status": "failed", "detail": f'_remote_submit_file: {detail_str}'}})
        except:
            logger.error(f'({file_type}) ! unable to update object to failed.')
        logger.error(f'({file_type}) ! updated object {agent_object_id} to failed.')


@app.post("/objects/load", summary="load object metadata and data for analysis from an end user or a 3rd party server",
          tags=["Post", "Service", "Data Provider Service", "Tool Service"])
async def post_object(parameters: ProviderParameters = Depends(ProviderParameters.as_form),
                      requested_object_id: Optional[str] = Query(None, title="Request an object id, not guaranteed. mainly for testing"),
                      optional_file_archive: UploadFile = File(None),
                      optional_file_expression: UploadFile = File(None),
                      optional_file_properties: UploadFile = File(None)
                      ):
    """
    warning: executing this repeatedly for the same service/object will create duplicates in the database
    example request_url: submitter_id=krobasky%40renci.org&data_type=dataset-geneExpression&version=1.0
    Service will be called repeatedly, once per file and once per accession_id, based on what is provided.
    Must provide either an accession_id or one of the three optional files to avoid a 500 error
    The ids for the submitted files map to fields under the agent metadata's field, "loaded_file_objects", as follows:
            "filetype_dataset_archive": optional_file_archive,
            "filetype_dataset_expression": optional_file_expression,
            "filetype_dataset_properties": optional_file_properties

    If specifying an accession_id, then also specify the filetype(s) to be retreived by providing non-empty strings for the upload files of the desired types. This functionality is temporary to accommodate a prototype and will be replaced later (see https://github.com/RENCI/fuse-agent/issues/5)
    """
    logger.info(f"parameters: {parameters}")
    try:
        # xxx be nice and assert config["configured-services"][parameters.service_id] exists, something like this:
        assert (parameters.service_id in _get_services())
        client_file_dict = {
            "filetype_dataset_archive": optional_file_archive,
            "filetype_dataset_expression": optional_file_expression,
            "filetype_dataset_properties": optional_file_properties
        }
        num_files_requested = 0
        for file_type in client_file_dict.keys():
            num_files_requested = num_files_requested + (client_file_dict[file_type] is not None)
        assert num_files_requested > 0 or parameters.accession_id is not None

        logger.info("record submitter ({parameters.submitter_id}), if not found create one")
        add_submitter_response = api_add_submitter(parameters.submitter_id)

        #####
        # xxx This code is common with /analyze, break it out:
        # Insert a new agent object
        logger.info(f"getting id")
        agent_object_id = _gen_object_id("agent", parameters.submitter_id, requested_object_id, mongo_objects)
        timeout_seconds = g_redis_default_timeout  # read this from config.json for the service xxx
        logger.info(f"submitter={parameters.submitter_id}, to service_id={parameters.service_id}, requesting {num_files_requested} files, timeout_seconds={timeout_seconds}")
        # stream any file(s) onto the fuse-agent server, named on fuse-agent from the client file name
        # xxx replace this with a ProviderObject model instance
        agent_object = {
            "object_id": agent_object_id,
            "created_time": datetime.utcnow(),
            "parameters": parameters.dict(),  # xxx?
            "agent_status": None,
            "detail": None,

            "service_host_url": None,  # url to provider, derived from params xxx - set this in callback
            "file_host_url": None,  # url to the results data server (dataset urls are in the parameters  xxx - set this in callback

            # "service_object_id": None, # id created on provider for this upload
            "loaded_file_objects": {},  # e.g., "filetype_dataset_expression": "upload_<email>_<remote_object_id>", ...
            "num_files_requested": num_files_requested
        }
        logger.info(f"inserting agent-side object for provider={agent_object}")
        _mongo_insert(mongo_objects, agent_object)
        logger.info(f"created provider object: object_id:{agent_object_id}, submitter_id:{parameters.submitter_id}")
        # END
        #####

        # unlink files and directory (which may be empty) when you get into the job
        agent_path = f"/app/data/{agent_object_id}-data"
        os.mkdir(agent_path)
        logger.info(f"upload file path = {agent_path}")

        if parameters.accession_id is not None:
            logger.info(f"calling service with accession id = {parameters.accession_id, parameters.apikey}")

        for file_type in client_file_dict.keys():
            logger.info(f"top of loop, file_type= {file_type}")  # ok so far

            client_file_obj = client_file_dict[file_type]
            agent_file_path = ""
            if client_file_obj is not None:
                client_file_name = client_file_obj.filename
                logger.info(f"getting file = {client_file_name}, file_type= {file_type}")  # ok so far
                if parameters.accession_id is None:  # xxx this is kludgey, revist (see https://github.com/RENCI/fuse-agent/issues/5)
                    agent_file_path = os.path.join(agent_path, client_file_name)
                    with open(agent_file_path, 'wb') as out_file:
                        contents = client_file_obj.file.read()
                        out_file.write(contents)
                else:
                    logger.info(f"accession_id provided, so no files uploaded for file_type={file_type}")

                # enqueue the job
                job_id = str(uuid.uuid4())
                # xxx maybe add to loaded_file_objects a status code and job_id?
                logger.info(f"QUEUE: agent_object_id:{agent_object_id}, file_type:{file_type}, agent_file_path:{agent_file_path},job_id=:{job_id}")  # ok so far
                g_queue.enqueue(_remote_submit_file,
                                args=(agent_object_id, file_type, agent_file_path, job_id),
                                timeout=timeout_seconds,
                                job_id=job_id,
                                result_ttl=-1)
                mongo_objects.update_one({"object_id": agent_object_id},
                                         {"$set": {
                                             "agent_status": "queued"
                                         }})

            logger.info(f"sleep a sec to try and avoid racing conditions inside _remote_submit_file")
            time.sleep(1)

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
            logger.error(f"! unable to change agent_object_id to agent_status=failed")
        logger.error(f"! {detail_str}")
        raise HTTPException(status_code=500, detail=f"! {detail_str}")


@app.get("/objects/search/{submitter_id}", summary="get all object_ids accessible for this submitter", tags=["Get", "Submitter"])
async def get_objects(submitter_id: str = Query(default=None, description="unique identifier for the submitter (e.g., email)")):
    """
    returns {'object_id': <object_id>},
    use /objects/{object_id} to get object status and other metadata
    """
    try:
        ret = list(map(lambda a: a, mongo_objects.find({"parameters.submitter_id": submitter_id}, {"_id": 0, "object_id": 1})))
        logger.info(f"ret:{ret}")
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


@app.get("/objects/url/{object_id}/type/{file_type}", summary="given a fuse-agent object_id, look up the metadata, find the DRS URI, parse out the URL to the file and return that",
         tags=["Get", "Service", "Data Provider Service", "Tool Service"])
async def get_url(object_id: str, file_type: FileType):
    """
    filetype is one of "filetype_dataset_archive", "filetype_dataset_expression", "filetype_dataset_properties", "filetype_results_cellularFunction",  or "filetype_results_PCATable"
    """
    try:
        # xxx make the parameter a FileType enum instead of a string
        logger.info(f"find local object={object_id}")
        entry = mongo_objects.find({"object_id": object_id}, {"_id": 0})
        assert _mongo_count(mongo_objects, {"object_id": object_id}) == 1
        obj = entry[0]
        logger.info(f'found local object: {obj}')
        assert obj["agent_status"] == "finished"

        # if the object was created from within a docker container, the service_host_url is going to be the container name, which you NEVER WANT for an externally accessible URL.
        # but you do want it for service calls across the docker network
        # how about creating a "file_host_url" field and populate it in /submit, /analyze with the config file
        obj_url = f'{obj["file_host_url"]}/files/{obj["loaded_file_objects"][file_type]["object_id"]}'
        logger.info(f"built url = ={obj_url}")
        return {"object_id": object_id, "url": obj_url}
    except Exception as e:
        detail_str = f'! Exception {type(e)} occurred while building url for ({object_id}), message=[{e}] ! traceback={traceback.format_exc()}'
        logger.error(detail_str)
        raise HTTPException(status_code=500, detail=detail_str)


def _agent_delete_object(agent_object_id: str):
    """
    Deletes agent object off local store; does NOT follow metadata to delete remote object.
    WARNING: call _remote_delete_object first in order to avoid orphaning cached objects that have been requested remotely
    returns:
       - array [info, stderr, status]: status can be one of "started", "deleted", "exception"
    """
    delete_status = "started"
    info_msg = ""
    stderr_msg = ""
    try:
        # Request delete on remote server
        # because this may run out-of-band, I think we might need a new mongodb connection?
        logger.warning(f"Deleting agent agent_object_id: {agent_object_id}")
        ret = mongo_objects.delete_one({"object_id": agent_object_id})
        # <class 'pymongo.results.DeleteResult'>
        delete_status = "deleted"
        if ret.acknowledged is not True:
            delete_status = "failed"
            info_msg += f"! Object not found on agent.\n{info_msg}"
            logger.error(f"agent delete failed, ret.acknowledged ! = True")
        if ret.deleted_count != 1:
            # should never happen if index was created for this field
            delete_status = "failed"
            info_msg += f"! Wrong number of records deleted from agent ({ret.deleted_count}).\n{info_msg}"
            logger.error(f"delete failed, wrong number deleted, count[1]={ret.deleted_count}")

        info_msg = f"{info_msg} Deleted agent objects, count=({ret.deleted_count}), Acknowledged=({ret.acknowledged})."
    except Exception as e:
        logger.error(f"Exception {type(e)} occurred while deleting agent {agent_object_id} from database, message=[{e}]  ! traceback={traceback.format_exc()}")
        stderr_msg += f"! Exception {type(e)} occurred while deleting agent {agent_object_id} from database, message=[{e}] ! traceback={traceback.format_exc()}"
        info_msg = f"! Somthing went wrong with local database delete.\n{info_msg}"
        delete_status = "exception"

    try:
        logger.info(f"Deleting cached files from agent {agent_object_id} off file system")
        local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
        local_path = os.path.join(local_path, f"{agent_object_id}-data")
        logger.info(f"removing tree ({local_path})")
        shutil.rmtree(local_path, ignore_errors=True)
    except Exception as e:
        logger.warning(f"Exception {type(e)} occurred while deleting agent {agent_object_id} from filesystem")
        stderr_msg += f"? Exception {type(e)} occurred while deleting agent object from filesystem, message=[{e}] ! traceback={traceback.format_exc()}"

    return [info_msg, stderr_msg, delete_status]


def _remote_delete_object(agent_object_id: str):
    """
    Deletes follows agent object id metadata to find and delete remote object. Does NOT delete agent object.
    WARNING: call _agent_delete_object after this call to avoid dangling references.
    returns array [info, stderr, status]
    status can be one of "started", "deleted", "not found", "failed"
    """
    delete_status = "started"
    info_msg = ''
    stderr_msg = ''
    try:
        # Request delete on remote server
        logger.info(f"connecting to {g_mongo_client_str} anew")
        '''
        # because this may run out-of-band, I think we might need a new mongodb connection?
        my_mongo_client = pymongo.MongoClient(g_mongo_client_str)
        my_mongo_db = my_mongo_client.test
        m_objects=my_mongo_db.objects
        # xxx close this connection?
        '''
        logger.info(f"looking up {agent_object_id}")
        entry = mongo_objects.find({"object_id": agent_object_id}, {"_id": 0})
        assert _mongo_count(mongo_objects, {"object_id": agent_object_id}) == 1
        obj = entry[0]

        num_deleted = 0
        for file_type in obj["loaded_file_objects"]:
            info_msg = f'{info_msg}Deleting object file_type={file_type}. '
            service_object_id = obj["loaded_file_objects"][file_type]["object_id"]
            # service_object_id = obj["service_object_id"] # set this early in case there's an exception
            host_url = _get_url(obj["parameters"]["service_id"])
            delete_url = f"{host_url}/delete/{service_object_id}"
            logger.info(f"delete remote object with {delete_url}")
            response = requests.delete(delete_url)
            assert response.status_code == 200 or response.status_code == 404
            if response.status_code == 404:
                info_msg = f"{info_msg}* Remote object not found when deleting {agent_object_id}. "
                logger.warning(f"* Remote object {service_object_id} not found with {delete_url} when deleting {agent_object_id}")
                delete_status = "not found"
                # xxx may want to add this to an audit report for admin or something
            else:
                num_deleted = num_deleted + 1

        if num_deleted == len(obj["loaded_file_objects"]):
            delete_status = "deleted"

    except Exception as e:
        logger.warning(f"Exception {type(e)} occurred while deleting {agent_object_id}")
        stderr_msg += f'? Exception {type(e)} occurred while deleting agent objec {agent_object_id} message=[{e}] ! traceback={traceback.format_exc()}'
        delete_status = "failed"

    ret_obj = [info_msg, stderr_msg, delete_status]
    logger.info(f"returning {ret_obj}")
    return ret_obj


# xxx connect this to delete associated analyses if object is dataset?
@app.delete("/delete/{object_id}", summary="DANGER ZONE: Delete a downloaded object; this action is rarely justified.",
            tags=["Delete", "Service", "Data Provider Service", "Tool Service"])
async def delete(object_id: str, force: bool = False):
    """
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
    """
    delete_status = "started"
    # concatenate these as you go so they can be reported out in the event of an exception
    info = ""
    stderr = ""
    try:
        logger.info(f"deleting {object_id}")

        # delete object off provider
        remote_status = ""
        try:
            logger.info(f"deleting {object_id} off remote service")
            # xxx may want to enqueue, but for now just call it directly
            # concatenate result to info, stderr
            [info, stderr, remote_status] = [orig + " " + new for orig, new in zip([info, stderr, ''], _remote_delete_object(object_id))]
            remote_status = remote_status.lstrip()
        except Exception as e:
            logger.warning(f"! exception while attempting to delete {object_id} off remote service")
            stderr += f'? Exception {type(e)} occurred while deleting provider object {object_id} message=[{e}] ! traceback={traceback.format_exc()}'

        # delete local object
        agent_status = ""
        try:
            if remote_status == "deleted" or remote_status == "not found" or force:
                logger.info(f"deleting agent object={object_id}")
                # concatenate result to info, stderr
                [info, stderr, agent_status] = [orig + " " + new for orig, new in zip([info, stderr, ''], _agent_delete_object(object_id))]
                delete_status = agent_status.lstrip()
            else:
                delete_status = remote_status
                logger.info(f"problem deleting object={object_id} on provider, status={remote_status}, info={info}, stderr={stderr}")
        except Exception as e:
            logger.error(f"! exception while attempting to delete {object_id} off local service")
            stderr += f'? Exception {type(e)} occurred while deleting agent object {object_id} message=[{e}] ! traceback={traceback.format_exc()}'
            delete_status = "failed"

        ret = {
            "status": delete_status,
            "info": info,
            "stderr": stderr
        }
        logger.info(f"returning=({ret})")

        assert delete_status == "deleted"
        return ret

    except Exception as e:
        detail_str = f'! Message=[{info}] Error while deleting ({object_id}), status=[{delete_status}] stderr=[{stderr}]'
        logger.error(f"Exception {type(e)} occurred while deleting agent {object_id} from filesystem. detail_str={detail_str}")
        raise HTTPException(status_code=404, detail=detail_str)


# xxx is this necessary? maybe just return status instead?
@app.get("/objects/{object_id}", summary="get metadata for the object", tags=["Get", "Service", "Data Provider Service", "Tool Service"])
async def get_object(object_id: str = Query(default=None, description="unique identifier on agent to retrieve previously loaded object")):
    """
    gets object's status and remote metadata
    Includes status and links to the input dataset, parameters, and dataset results if this object was created by a tool service
    """
    try:
        # xxx retrieve metadata for each file type listed in loaded_file_objects
        logger.info(f"Finding metadata for object {object_id}")
        entry = mongo_objects.find({"object_id": object_id}, {"_id": 0})
        assert _mongo_count(mongo_objects, {"object_id": object_id}) == 1
        obj = entry[0]
        logger.info(f'found local object, agent_status={obj["agent_status"]}')
        service_obj_metadata = None
        new_obj = {}
        new_obj["agent"] = obj
        new_obj["provider"] = {}
        if obj["agent_status"] == "finished":
            logger.info(f'obj=({obj})')
            for file_type in obj["loaded_file_objects"]:
                logger.info(f'***********file_type =({file_type})')
                logger.info(f'***********obj =({obj})')
                service_object_id = obj["loaded_file_objects"][file_type]["object_id"]
                service_host_url = obj["loaded_file_objects"][file_type]["service_host_url"]
                logger.info(f'({file_type}) REQUEST: {service_host_url}/objects/{service_object_id}')
                response = requests.get(f'{service_host_url}/objects/{service_object_id}')
                service_obj_metadata = response.json()
                logger.info(f'({file_type}) METADATA={service_obj_metadata}')
                new_obj["provider"][file_type] = service_obj_metadata  # xxx need to fill this in during queue

        return new_obj

    except Exception as e:
        raise HTTPException(status_code=404,
                            detail=f"! Exception {type(e)} occurred while retrieving metadata for ({object_id}), message=[{e}] ! traceback={traceback.format_exc()}")


async def _remote_analyze_object(agent_object_id: str, parameters: ToolParameters):
    try:
        # INIT #####################################################################################################################
        logger.info(f"connecting to {g_mongo_client_str} anew; agent_object_id:{agent_object_id} ")
        # xxx take this out?:
        my_mongo_client = pymongo.MongoClient(g_mongo_client_str)
        my_mongo_db = my_mongo_client.test
        m_objects = my_mongo_db.objects
        detail_str = ""
        timeout_seconds = g_redis_default_timeout  # xxx read this from config.json, what's reasonable here?
        # get agent object
        logger.info(f"looking up {agent_object_id}")
        entry = m_objects.find({"object_id": agent_object_id}, {"_id": 0})
        assert _mongo_count(m_objects, {"object_id": agent_object_id}) == 1
        obj = entry[0]
        required_in_file_types = _get_service_value(obj["parameters"]["service_id"], ServiceIOType.datasetInput, ServiceIOField.fileType)
        # update to initialize agent object
        m_objects.update_one({"object_id": agent_object_id},
                             {"$set": {
                                 "service_host_url": _get_url(obj["parameters"]["service_id"]),
                                 "file_host_url": _get_url(obj["parameters"]["results_provider_service_id"], "file_url", "results-provider-services"),
                                 "num_files_requested": len(required_in_file_types),
                                 "agent_status": "started"
                             }})
        logger.info(f'agent results object updated with tool and results server urls')
        # 1. get the data object(s) requested in the parameters #####################################################################
        try:
            entry = m_objects.find({"object_id": obj["parameters"]["dataset"]})
            dataset_obj = entry[0]
            logger.info(f'found one; file_host_url = {dataset_obj["file_host_url"]}')
            assert _mongo_count(m_objects, {"object_id": obj["parameters"]["dataset"]}) == 1
            # assert that the requested dataset data_type and files matche tool's inputDatasetType parameters of the requested tool
            for file_type in required_in_file_types:
                assert file_type in dataset_obj["loaded_file_objects"]
        except Exception as e:
            raise logger.error(
                f'! Exception {type(e)} occurred while attempting to collate dataset urls for ({agent_object_id}), parameters=({obj["parameters"]}) message=[{e}] ! traceback={traceback.format_exc()}')

        logger.info(f'params={json.dumps(obj["parameters"])}')

        # 2. post the dataset to the analysis endpoint ##################################################################################

        # fill in the analyze endpoint parameters with dataset urls
        for file_type in required_in_file_types:
            # for each file_type, remove 'filetype_dataset_', append '_url' to get the relevant tool parameter name
            param_name = f'{file_type.removeprefix("filetype_dataset_")}_url'
            logger.info(f'param_name={param_name}')
            # xxx the provider host url is <localhost> for non-container users
            # xxx but it's <container-name> for containerized tools.
            # so if there's a 'container_url configured, use that, otherwise default to the 'file_url'
            container_url = _get_url(dataset_obj["parameters"]["service_id"], "container_url")
            if container_url is not None:
                file_url = f'{container_url}/files/{dataset_obj["loaded_file_objects"][file_type]["object_id"]}'
            else:
                file_url = f'{dataset_obj["file_host_url"]}/files/{dataset_obj["loaded_file_objects"][file_type]["object_id"]}'
            logger.info(f'file_url={file_url}')
            obj["parameters"][param_name] = file_url

        logger.info(f'parameters={obj["parameters"]}')
        logger.info(f'submit_url={_get_url(obj["parameters"]["service_id"])}/submit')
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
        headers = {'accept': next(iter(_get_service_value(obj["parameters"]["service_id"], ServiceIOType.resultsOutput, ServiceIOField.mimeType)))}
        logger.info(f"headers: {headers}")
        analysis_request_url = f"{_get_url(obj['parameters']['service_id'])}/submit"
        params = obj["parameters"]
        params['expression_file'] = None
        logger.info(f"analysis_request_url: {analysis_request_url}, params: {params}")
        analysis_response = requests.post(analysis_request_url, data=params, headers=headers)
        logger.info(f'analysis_response.status_code: {analysis_response.status_code}')
        if analysis_response.status_code != 200:
            raise Exception(f"Failed to successfully post request to: {analysis_request_url}")

        # 3. store the results object in the results service ##################################################################
        # save results to /tmp
        # xxx maybe replace this with: {'client_file': open(io.StringIO(str(response.content,'utf-8')), 'rb')}
        results = analysis_response.json()
        logger.info(f"submitter_id: {results['submitter_id']}, start_time: {results['start_time']}, end_time: {results['end_time']}")

        loaded_file_objects = {}
        data = results['results']

        service_info = _get_service_info(obj["parameters"]["service_id"])
        for output in service_info[ServiceIOType.resultsOutput]:
            file_extension = output[ServiceIOField.fileExt]
            results_type = output[ServiceIOField.fileType]
            for entry in data:
                if entry['results_type'] != results_type:
                    continue
                results_file_name = f'{agent_object_id}_{entry["name"]}.{file_extension}'
                results_file_path = f'/tmp/{results_file_name}'
                with open(results_file_path, 'wb') as s:
                    for line in entry['data']:
                        s.write(','.join(line).encode("utf-8"))
                        s.write('\n'.encode('utf-8'))
                s.close()
                logger.info(f'wrote response to {results_file_path}')

                # create results upload args
                files = {'client_file': (f'{results_file_name}', open(results_file_path, 'rb'))}  # xxx make this a zipped file
                headers = {'accept': output[ServiceIOField.mimeType]}
                # fill in the parameters for the results
                params = {
                    "service_id": "fuse-provider-upload",
                    "submitter_id": obj["parameters"]["submitter_id"],
                    "data_type": output[ServiceIOField.dataType],
                    "file_type": output[ServiceIOField.fileType],
                    "version": "1.0"
                }
                # build results upload url
                results_provider_host_url = _get_url(obj["parameters"]["results_provider_service_id"], "service_url", "results-provider-services")
                logger.info(f'results_provider_host_url: {results_provider_host_url}, params: {params}')
                # call upload provider
                store_response = requests.post(f"{results_provider_host_url}/submit", data=params, headers=headers, files=files)
                logger.info(f'object added to results server, status code=({store_response.status_code})')
                if analysis_response.status_code != 200:
                    raise Exception(f"Failed to successfully post request to: {results_provider_host_url}")
                # read the respones
                logger.info(f"store_response.content: {store_response.content}")
                store_obj = store_response.json()
                # xxx if the results file is a zip, add 'loaded files' meta data about the files here
                # unlink the /tmp file(s)
                os.unlink(results_file_path)
                loaded_file_objects[results_type] = {
                    "object_id": store_obj["object_id"],
                    "service_host_url": _get_url(obj["parameters"]["results_provider_service_id"]),
                    "file_host_url": _get_url(obj["parameters"]["results_provider_service_id"], "file_url")
                }

        logger.info(f'*********  loaded_file_objects = {loaded_file_objects}')
        m_objects.update_one({"object_id": agent_object_id},
                             {"$set": {
                                 "loaded_file_objects": loaded_file_objects,
                                 "agent_status": "finished"  # maybe add detail? xxx
                             }})
        # xxx need to fix the /files endpoint to return url to the single resutls file
        logger.info(f"****DONE.*****")
        return
    except Exception as e:
        detail_str += f"! Exception {type(e)} occurred while analyzing dataset, message=[{e}] ! traceback={traceback.format_exc()}"
        logger.error(f"! status=failed, {detail_str}")
        try:
            m_objects.update_one({"object_id": agent_object_id},
                                 {"$set": {
                                     "agent_status": "failed",
                                     "detail": f'_remote_analyze_object: {detail_str}'
                                 }})
        except:
            logger.error(f'({agent_object_id}) ! unable to update object to failed.')
        logger.error(f'! updated object {agent_object_id} to failed.')


@app.post("/analyze", summary="submit an analysis", tags=["Post", "Service", "Tool Service"])
async def analyze(parameters: ToolParameters = Depends(ToolParameters.as_form),
                  requested_results_object_id: Optional[str] = Query(None, title="Request an object id for the results, not guaranteed. mainly for testing")):
    """
from slack:
When an analysis is submitted to an agent, the agent will:
Create a "results"-type object_id in it's database, containing status="started" and the link to where you can get the object when its finished
enqueue the tool request
return the object_id
2. When the analysis job comes up, the agent updates the status, calls the tool, waits for a result,  and persists the result
3. When the dashboard asks for the object, the agent returns the meta data
4. If the meta data shows status = finished, the dashboard uses the link in the meta data to retrieve the results. (edited)
    """
    try:
        detail_str = ""
        dataset_object_id = None
        requested_object_id = requested_results_object_id
        logger.info(f"if record for this submitter ({parameters.submitter_id}) is not found, create one")
        add_submitter_response = api_add_submitter(parameters.submitter_id)
        # quick check that dataset exist in the db
        dataset_object_id = parameters.dataset
        logger.info(f"checking that dataset {dataset_object_id} for analysis are legit")
        assert _mongo_count(mongo_objects, {"object_id": dataset_object_id}) == 1

        # add submitter
        logger.info(f"getting id")
        agent_object_id = _gen_object_id("agent", parameters.submitter_id, requested_object_id, mongo_objects)
        timeout_seconds = g_redis_default_timeout  # read this from config.json for the service xxx
        # xxx replace this with a AgentObject model instance;
        results_provider_host_url = ''
        agent_object = {
            "object_id": agent_object_id,
            "created_time": datetime.utcnow(),
            "parameters": parameters.dict(),  # parameters used for creating the results xxx works?
            "agent_status": None,  # status of the analyses
            "detail": None,  # any error messages
            "service_host_url": None,  # url to tool, derived from params
            "file_host_url": None,  # url to the results data server (dataset urls are in the parameters
            "loaded_file_objects": {},  # e.g., "filetype_results_PCATable": "..."
            "num_files_requested": 0
        }
        logger.info(f"inserting agent-side agent_object={agent_object}")
        _mongo_insert(mongo_objects, agent_object)
        logger.info(f"created agent object: object_id:{agent_object_id}, submitter_id:{parameters.submitter_id}")

        # enqueue the job
        job_id = str(uuid.uuid4())
        logger.info(f"submitter={parameters.submitter_id}, to service_id={parameters.service_id}, timeout_seconds={timeout_seconds}, job_id={job_id}")
        g_queue.enqueue(_remote_analyze_object, args=(agent_object_id, parameters), timeout=timeout_seconds, job_id=job_id, result_ttl=-1)
        logger.info(f"Updating status")
        mongo_objects.update_one({"object_id": agent_object_id},
                                 {"$set": {"agent_status": "queued"}})

        return {
            "object_id": agent_object_id,
            "submitter_action_status": add_submitter_response["submitter_action_status"]
        }

    except Exception as e:
        raise HTTPException(status_code=404,
                            detail=f"! (dataset_object_id={dataset_object_id}) Exception {type(e)} occurred while running submit, message=[{e}] ! traceback={traceback.format_exc()}")


if __name__ == '__main__':
    uvicorn.run("main:app", host='0.0.0.0', port=int(os.getenv("HOST_PORT")), reload=True)
