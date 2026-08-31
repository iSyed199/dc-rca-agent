from __future__ import annotations
from datetime import datetime
from typing import Literal, Optional, Dict, Any
from pydantic import BaseModel, Field

Status = Literal["SUCCESS", "FAILURE"]
StageName = Literal["INIT", "SCRIPT", "GENMCF", "VALIDATION", "FINISH"]

# Statuses representing in-progress or unresolved audits that we want to re-process when new logs occur
REPROCESSABLE_STATUSES = ["NO_RUN_FOLDERS_FOUND", "PENDING", "RUNNING", "TRIAGING", "PENDING_SYNC"]

class LogPayload(BaseModel):
    log_type: str = Field(..., alias="log_type")
    status: Status
    message: str
    import_name: Optional[str] = None
    stage_name: Optional[StageName] = None
    stage: Optional[str] = None
    latency: Optional[float] = None
    latency_secs: Optional[float] = None
    data_bytes: Optional[int] = None

class ResourceLabels(BaseModel):
    resource_container: str
    location: str
    job_id: str

class Resource(BaseModel):
    type: str
    labels: ResourceLabels

class LogLabels(BaseModel):
    task_group_name: Optional[str] = None
    job_uid: str

class LogEntry(BaseModel):
    logName: str
    resource: Resource
    jsonPayload: LogPayload
    timestamp: datetime
    labels: LogLabels

class PubsubMessage(BaseModel):
    attributes: Dict[str, str] = {}
    data: str  # Base64 encoded JSON log entry
    messageId: str
    publishTime: datetime

class PubsubPushRequest(BaseModel):
    message: PubsubMessage
    subscription: str

class FailureEvent(BaseModel):
    job_id: str  # e.g., worlddevelopmentin-047f6f5d-f21d-4b630
    job_uid: str  # e.g., worlddevelopmentin-047f6f5d-f21d-4b630
    import_name: str
    stage_name: StageName
    status: Status
    message: str
    timestamp: datetime  # Log event timestamp (UTC)
    gcs_path: Optional[str] = None
    job_name: Optional[str] = None
    region: Optional[str] = None
    project_id: Optional[str] = None
    log_diagnosis: Optional[Dict[str, Any]] = None
