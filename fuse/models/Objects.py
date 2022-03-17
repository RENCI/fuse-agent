# xxx use this in the code
class FileType(str, Enum):
    dataset-archive: "filetype-dataset-archive"
    dataset-expression: "filetype-dataset-expression"
    dataset-properties: "filetype-dataset-properties"
    results-pca: "filetype-results-pca"

# xxx use this in the code
# xxx will 'None' type cause this to be set? If so, need to fix code that counts loaded files via len(FileObject)
Class FileObject:
    filetype-dataset-archive: Optional[str] = None
    filetype-dataset-expression: Optional[str] = None
    filetype-dataset-properties: Optional[str] = None
    
# xxx use this in the code
class ProviderDataset:
    '''
    A set of one or more objects that have been loaded into the 3rd-party provider, bundled together based on how the original submitter loaded them.
    '''
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
    '''
    A list of one or more of the files from a specific ProviderDataset loaded by the agent
    '''
    provider_dataset_object_id: str
    file_types: List[FileType]
