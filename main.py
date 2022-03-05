import datetime
import dateutil.parser
import inspect
import os

from fastapi import FastAPI, File, UploadFile, Form, Depends, HTTPException, Query
from fastapi.logger import logger
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Type, Optional, List
from starlette.responses import StreamingResponse

from bson.json_util import dumps, loads

import traceback

from logging.config import dictConfig
import logging
from fuse.models.Config import LogConfig

dictConfig(LogConfig().dict())
logger = logging.getLogger("fuse-agent")


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
        logger.info(msg=f"[add_submitter]found ("+str(entry.count())+") matches for object_id="+str(object_id))
        if entry.count() != 0:
            raise Exception("Submitter already added as: " + str(object_id)+", entries found = "+ str(entry.count()))

        submitter_object = {
            "object_id": object_id,
            "submitter_id": submitter_id,
            "created_time": datetime.datetime.utcnow(),
            "status": "active"
        }
        logger.info(msg=f"[add_submitter] submitter_object="+str(submitter_object))
        mongo_submitters.insert(submitter_object)
        logger.info(msg=f"[add_submitter] submitter added.")

        ret_val = {"submitter_id": submitter_id}
        logger.info(msg=f"[add_submitter] returning: " + str(ret_val))
        return ret_val
    except Exception as e:
        logger.info(msg=f"[add_submitter] exception, setting upload status to failed for "+object_id)
        raise HTTPException(status_code=404,
                            detail="! Exception {0} occurred while inserting submitter ({1}), message=[{2}] \n! traceback=\n{3}\n".format(type(e), submitter_id, e, traceback.format_exc()))
        

@app.get("/submitter/{submitter_id}", summary="Return metadata associated with submitter")
async def get_submitter(submitter_id: str = Query(default=None, description="unique identifier for the submitter (e.g., email)")):
    try:
        object_id = _submitter_object_id(submitter_id)
        entry = mongo_submitters.find({"object_id": object_id},{"_id":0})
        logger.info(msg=f"[submitter]found ("+str(entry.count())+") matches for object_id="+str(object_id))
        if entry.count() != 1:
            raise Exception("Wrong number of submitters found for [" + str(object_id)+"], entries found = "+ str(entry.count()))
        ret_val = entry[0]
        logger.info(msg=f"[submitter] returning: " + str(ret_val))
        return ret_val
    except Exception as e:
        raise HTTPException(status_code=404,
                            detail="! Exception {0} occurred while finding submitter ({1}), message=[{2}] \n! traceback=\n{3}\n".format(type(e), submitter_id, e, traceback.format_exc()))
    
    
@app.get("/search/submitters", summary="Return a list of known submitters")
async def get_submitters(within_minutes: Optional[int] = Query(default=None, description="find submitters created within the number of specified minutes from now")):
    '''
    return list of submitters
    '''
    try:
        if within_minutes != None:
            logger.info(msg=f"[submitters] get submitters created within the last %d minutes." % within_minutes)
            until_time = datetime.datetime.utcnow()
            within_minutes_time = datetime.timedelta(minutes=within_minutes)
            from_time = until_time - within_minutes_time
            search_object = {
                "created_time": {
                    "$gte": from_time,
                    "$lt": until_time
                }
            }
        else:
            logger.info(msg=f"[submitters] get all.")
            search_object = {}
        ret = list(map(lambda a: a, mongo_submitters.find(search_object, {"_id": 0, "submitter_id": 1})))
        logger.info(msg=f"[submitters] ret:" + str(ret))
        return ret
    
    except Exception as e:
        raise HTTPException(status_code=404,
                            detail="! Exception {0} occurred while searching submitters, message=[{1}] \n! traceback=\n{2}\n".format(type(e), e, traceback.format_exc()))

    
@app.delete("/delete/submitter/{submitter_id}", summary="Remove a submitter record")
async def delete_submitter(submitter_id: str= Query(default=None, description="unique identifier for the submitter (e.g., email)")):
    '''
    deletes submitter and their datasets, analyses
    '''
    delete_status = "done"
    ret_mongo=""
    ret_mongo_err=""
    try:
        logger.warn(msg=f"[delete_submitter] Deleting submitter_id:" + str(submitter_id))
        ret = mongo_submitters.delete_one({"submitter_id": submitter_id})
        #<class 'pymongo.results.DeleteResult'>
        delete_status = "deleted"
        if ret.acknowledged != True:
            delete_status = "failed"
            ret_mongo += "ret.acknoledged not True.\n"
            logger.error(msg=f"[delete_submitter] delete failed, ret.acknowledged ! = True")
        if ret.deleted_count != 1:
            # should never happen if index was created for this field
            delete_status = "failed"
            ret_mongo += "Wrong number of records deleted ("+str(ret.deleted_count)+")./n"
            logger.error(msg=f"[delete_submitter] delete failed, wrong number deleted, count[1]="+str(ret.deleted_count))
        ## xxx
        # could check if there are any remaining; but this should instead be enforced by creating an index for this columnxs
        # could check ret.raw_result['n'] and ['ok'], but 'ok' seems to always be 1.0, and 'n' is the same as deleted_count
        ##
        ret_mongo += "Deleted count=("+str(ret.deleted_count)+"), Acknowledged=("+str(ret.acknowledged)+")./n"
    except Exception as e:
        logger.error(msg=f"[delete_submitter] Exception {0} occurred while deleting {1} from database\n".format(type(e), submitter_id))
        ret_mongo_err += "! Exception {0} occurred while deleting {1} from database, message=[{2}] \n! traceback=\n{3}\n".format(type(e), submitter_id, e, traceback.format_exc())
        delete_status = "exception"
        
    ret = {
        "status": delete_status,
        "info": ret_mongo,
        "stderr": ret_mongo_err
    }
    logger.info(msg=f"[delete_submitter] returning ("+str(ret)+")\n")
    return ret
    
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
from slack:
When an analysis is submitted to an agent, the agent will:
Create a "results"-type object_id in it's database, containing status="started" and the link to where you can get the object when its finished
enqueue the tool request
return the object_id
2. When the analysis job comes up, the agent updates the status, calls the tool, waits for a result,  and persists the result
3. When the dashboard asks for the object, the agent returns the meta data
4. If the meta data shows status = finished, the dashboard uses the link in the meta data to retrieve the results. (edited) 
    '''


@app.get("/search/objects", summary="get all object metadata accessible for this submitter")
async def get_results(submitter_id: Optional[str] = Query(default=None, description="unique identifier for the submitter (e.g., email)")):
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
