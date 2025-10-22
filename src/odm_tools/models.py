import json
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Literal

from geojson_pydantic import MultiPolygon, Polygon
from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel


class DataType(Enum):
    rgb = 22002
    thermal = 22001


class TaskStatus(str, Enum):
    queued = "QUEUED"
    running = "RUNNING"
    failed = "FAILED"
    completed = "COMPLETED"
    canceled = "CANCELED"


class TaskTracker(BaseModel):
    pyodm_task_id: str
    request_id: str
    datatypes: dict[int, str]
    created_at: datetime = Field(default_factory=datetime.now)
    output_path: Path | None = None


class ProcessingRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    start: datetime
    end: datetime
    request_id: str = Field(alias="requestId")
    situation_id: str = Field(alias="situationId")
    datatype_ids: list[int] = Field(alias="datatypeIds")
    feature: Polygon
    file_path: Path

    @classmethod
    def from_file(cls, path: Path) -> "ProcessingRequest":
        with open(path) as f:
            data = json.load(f)
        return cls(**data, file_path=path)

    @property
    def path(self) -> Path:
        return self.file_path.parent


class ODMTask(BaseModel):
    task_id: str
    request_id: str
    datatype_id: int
    status: TaskStatus
    progress: int = 0
    processing_time: int = 0
    output_path: Path | None = None
    error_message: str | None = None
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)


class ODMServerInfo(BaseModel):
    version: str
    task_queue_count: int = 0
    available_memory: int | None = None
    max_parallel_tasks: int = 1

    @classmethod
    def from_api_response(cls, data: dict) -> "ODMServerInfo":
        return cls(
            version=data.get("version", "unknown"),
            task_queue_count=data.get("taskQueueCount", 0),
            available_memory=data.get("availableMemory"),
            max_parallel_tasks=data.get("maxParallelTasks", 1),
        )


class StatusUpdate(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, validate_by_name=True)
    request_id: str
    datatype_id: int
    status: Literal["start", "update", "end", "error"]
    timestamp: datetime
    message: str


# https://docs.ckan.org/en/2.10/api/#ckan.logic.action.create.resource_create
class ResourceCreateRequest(BaseModel):
    package_id: str
    datatype_resource: int
    file_date_start: datetime
    file_date_end: datetime
    format: str
    name: str


class MetadataINSPIRE(BaseModel):
    model_config = ConfigDict(validate_by_name=True)
    title: str = ""
    private: bool = True
    notes: str = ""
    name: str = ""
    ident_resource_type: str = Field("dataset", alias="identification_ResourceType")
    owner_org: str = ""
    ident_coupled_resource: str = Field("", alias="identification_CoupledResource")
    ident_res_language: str = Field("eng", alias="identification_ResourceLanguage")
    classification_category: str = Field("imageryBaseMapsEarthCover", alias="classification_TopicCategory")
    classification_spatial_dst: str = Field("", alias="classification_SpatialDataServiceType")
    keyword: str = Field("", alias="keyword_KeywordValue")
    keyword_vocabulary: str = Field("ontology", alias="keyword_OriginatingControlledVocabulary")
    data_temporal_extent_begin_date: str = ""
    data_temporal_extent_end_date: str = ""
    tref_date_publication: str = Field("", alias="temporalReference_dateOfPublication")
    tref_date_revision: str = Field("", alias="temporalReference_dateOfLastRevision")
    tref_date_creation: str = Field("", alias="temporalReference_dateOfCreation")
    tref_date: str = Field("", alias="temporalReference_date")
    quality_and_validity_lineage: str = "Quality approved"
    quality_and_validity_spatial_resolution_latitude: str = ""
    quality_and_validity_spatial_resolution_longitude: str = ""
    quality_and_validity_spatial_resolution_scale: str = "0"
    quality_and_validity_spatial_resolution_measureunit: str = "m"
    conformity_specification_title: str = "COMMISSION REGULATION (EU) No 1089/2010 of 23 November 2010 implementing Directive 2007/2/EC of the European Parliament and of the Council as regards interoperability of spatial data sets and services"
    conformity_specification_date_type: str = Field("publication", alias="conformity_specification_dateType")
    conformity_specification_date: str = "2010-12-08T00:00:00"
    conformity_degree: bool = True
    constraints_conditions_for_access_and_use: str = "Creative Commons CC BY-SA 3.0 IGO licence"
    constraints_limitation_on_public_access: str = ""
    responsable_organization_name: str = ""
    responsable_organization_email: str = ""
    responsable_organization_role: str = "author"
    point_of_contact_name: str = ""
    point_of_contact_email: str = ""
    metadata_language: str = "eng"
    coordinatesystemreference_code: int = 4326
    coordinatesystemreference_codespace: str = "EPSG"
    character_encoding: str = "UTF-8"
    spatial: Polygon | MultiPolygon | None = None
    request_code: str = ""
    destinatary_organization: str = ""
    external_attributes: dict = {}
