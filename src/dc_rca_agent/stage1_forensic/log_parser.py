import base64
import json
import logging
from ..models import LogEntry, PubsubPushRequest, FailureEvent

log = logging.getLogger(__name__)

class NotAFailureEvent(Exception):
    """Raised when a log entry doesn't match the RCA trigger criteria."""
    pass

def parse_pubsub_push(request: PubsubPushRequest) -> LogEntry:
    # Decode base64 data
    decoded_data = base64.b64decode(request.message.data).decode('utf-8')
    log_entry_dict = json.loads(decoded_data)
    return LogEntry.model_validate(log_entry_dict)

def to_failure_event(entry: LogEntry) -> FailureEvent:
    payload = entry.jsonPayload
    
    # Resolve stage_name dynamically
    stage_name = payload.stage_name or payload.stage
    if not stage_name:
        raise NotAFailureEvent("No stage name found in payload")
        
    # We trigger on:
    # 1. Any FAILURE or VALIDATION event.
    # 2. SUCCESS events for the FINISH stage (which represents a completion of the entire run).
    is_finish_success = (payload.status == "SUCCESS" and stage_name == "FINISH")
    
    if payload.status not in ["FAILURE", "VALIDATION"] and not is_finish_success:
        raise NotAFailureEvent(
            f"Skip event: status={payload.status}, stage={stage_name}"
        )
        
    # Resolve import_name dynamically
    import_name = payload.import_name
    if not import_name:
        if payload.message and "Import: " in payload.message:
            import_name = payload.message.split("Import: ")[1].split(",")[0].strip()
            
    if not import_name:
        raise NotAFailureEvent(f"No import name found in payload: {payload.message}")
        
    job_name = None
    if entry.labels.task_group_name:
        parts = entry.labels.task_group_name.split('/jobs/')
        if len(parts) > 1:
            job_name = parts[1].split('/')[0]
        
    proj = entry.resource.labels.resource_container
    if proj and proj.startswith("projects/"):
        proj = proj.replace("projects/", "")
        
    loc = entry.resource.labels.location
    region = loc
    if loc:
        parts = loc.split('-')
        if len(parts) > 2:
            region = "-".join(parts[:-1])
            
    return FailureEvent(
        job_id=entry.resource.labels.job_id,
        job_uid=entry.labels.job_uid,
        import_name=import_name,
        stage_name=stage_name,
        status=payload.status,
        message=payload.message,
        timestamp=entry.timestamp,
        job_name=job_name,
        region=region,
        project_id=proj
    )
