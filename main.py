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

