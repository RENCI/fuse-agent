from typing import List, Optional

from fastapi import Query
from fuse_cdm.main import Checksums, as_form, DataType
from pydantic import BaseModel, Field, EmailStr


@as_form
class AgentProviderParameters(BaseModel):
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
    requested_object_id: str = Query(default=None,
                                     description="optional argument to be used by submitter to request an object_id; this could be, for example, used to retrieve objects from a 3rd party for which this endpoint is a proxy. The requested object_id is not guaranteed, enduser should check return value for final object_id used.")
