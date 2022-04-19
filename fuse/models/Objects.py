import inspect
import json
from datetime import datetime
from enum import Enum
from typing import Type, List, Optional

from fastapi import Form
from pydantic import BaseModel, AnyUrl, HttpUrl, EmailStr, Field, AnyHttpUrl


def config():
    with open("/app/config.json") as f:
        return json.load(f)


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


class Checksums(BaseModel):
    checksum: str
    type: str


class AccessURL(BaseModel):
    url: AnyUrl = None
    headers: str = None


class AccessMethods(BaseModel):
    type: str = None
    access_url: AccessURL = None
    access_id: str = None
    region: str = None


class Contents(BaseModel):
    name: str = None
    id: str = None
    drs_uri: AnyUrl = None
    contents: List[str] = []


class JobStatus(str, Enum):
    started = 'started'
    failed = 'failed'
    finished = 'finished'


class Service(BaseModel):
    id: str
    title: str = None
    URL: HttpUrl = None


class SubmitterActionStatus(str, Enum):
    unknown = 'unknown'
    created = 'created'
    existed = 'existed'


class SubmitterStatus(str, Enum):
    requested = 'requested'
    approved = 'approved'
    disabled = 'disabled'


@as_form
class Submitter(BaseModel):
    object_id: str = None
    submitter_id: EmailStr = None
    created_time: datetime = None
    status: SubmitterStatus = SubmitterStatus.requested


class FileType(str, Enum):
    dataset_archive = 'filetype-dataset-archive'
    dataset_expression = 'filetype-dataset-expression'
    dataset_properties = 'filetype-dataset-properties'
    results_pca = 'filetype-results-pca'


class FileObject:
    filetype_dataset_archive: Optional[str] = None
    filetype_dataset_expression: Optional[str] = None
    filetype_dataset_properties: Optional[str] = None


class ServiceIOType(str, Enum):
    datasetInput = 'inputDatasetType'
    resultsOutput = 'outputResultsType'


class ServiceIOField(str, Enum):
    dataType = 'data_type'
    fileTypes = 'file_types'
    fileType = 'file_type'
    mimeType = 'mime_type'
    fileExt = 'file_extension'


class ProviderDataset:
    """
    A set of one or more objects that have been loaded into the 3rd-party provider, bundled together based on how the original submitter loaded them.
    """
    object_id: str
    created_time: datetime
    parameters: dict
    agent_status: JobStatus
    detail: str
    service_object_id: str
    service_host_url: HttpUrl
    loaded_file_objects: List[FileObject]
    num_files_requested: int


class ToolDataset:
    """
    A list of one or more of the files from a specific ProviderDataset loaded by the agent
    """
    provider_dataset_object_id: str
    file_types: List[FileType]


@as_form
class ToolParameters(BaseModel):
    service_id: str
    submitter_id: EmailStr = Field(..., title="email", description="unique submitter id (email)")
    number_of_components: Optional[int] = 3
    dataset: str = Field(...)
    description: Optional[str] = Field(None, title="Description", description="detailed description of the requested analysis being performed (optional)")
    expression_url: Optional[AnyHttpUrl] = Field(None, title="Gene expression URL", description="Optionally grab expression from an URL instead of uploading a file")
    properties_url: Optional[AnyHttpUrl] = Field(None, title="Properties URL", description="Optionally grab properties from an URL instead of uploading a file")
    archive_url: Optional[AnyHttpUrl] = Field(None, title="Archive URL", description="Optionally grab all the files from an URL to an archive instead of uploading file(s)")
    results_provider_service_id: Optional[str] = Field(config()["results-provider-services"]["default"], title="Data Provider for Results",
                                                       description="If not set, the system default will be provided. e.g., 'fuse-provider-upload'")


# xxx cdm
from enum import Enum


class DataType(str, Enum):
    geneExpression = 'class_dataset_expression'
    resultsPCATable = 'class_results_PCATable'
    resultsCellFIE = 'class_results_CellFIE'
    # xxx to add more datatypes: expand this


class FileType(str, Enum):
    datasetGeneExpression = 'filetype_dataset_expression'
    datasetProperties = 'filetype_dataset_properties'
    datasetArchive = 'filetype_dataset_archive'
    resultsPCATable = 'filetype_results_PCATable'
    resultsCellFIE = 'filetype_results_CellFIE'
    # xxx to add more datatypes: expand this


@as_form
class ProviderParameters(BaseModel):
    service_id: str = Field(..., title="Provider service id", description="id of service used to upload this object")
    submitter_id: EmailStr = Field(..., title="email", description="unique submitter id (email)")
    data_type: DataType = Field(..., title="Data type of this object", description="the type of data associated with this object (e.g, results or input dataset)")
    description: Optional[str] = Field(None, title="Description", description="detailed description of this data (optional)")
    version: Optional[str] = Field(None, title="Version of this object",
                                   description="objects shouldn't ever be deleted unless data are redacted or there is a database consistency problem.")
    accession_id: Optional[str] = Field(None, title="External accession ID", description="if sourced from a 3rd party, this is the accession ID on that db")
    apikey: Optional[str] = Field(None, title="External apikey", description="if sourced from a 3rd party, this is the apikey used for retrieval")
    aliases: Optional[str] = Field(None, title="Optional list of aliases for this object")
    checksums: Optional[List[Checksums]] = Field(None, title="Optional checksums for the object",
                                                 description="enables verification checking by clients; this is a json list of objects, each object contains 'checksum' and 'type' fields, where 'type' might be 'sha-256' for example.")
