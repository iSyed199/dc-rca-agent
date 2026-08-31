import base64
import json
import pytest
from datetime import datetime, timezone
from dc_rca_agent.models import PubsubPushRequest, PubsubMessage
from dc_rca_agent.stage1_forensic.log_parser import (
    parse_pubsub_push,
    to_failure_event,
    NotAFailureEvent
)
from tests.fixtures.log_events import INIT_ENTRY, VALIDATION_FAILURE_ENTRY

def _build_push_request(log_payload: dict) -> PubsubPushRequest:
    raw_payload_str = json.dumps(log_payload)
    encoded_payload = base64.b64encode(raw_payload_str.encode('utf-8')).decode('utf-8')
    
    return PubsubPushRequest(
        message=PubsubMessage(
            messageId="12345",
            publishTime=datetime.now(timezone.utc),
            data=encoded_payload
        ),
        subscription="projects/test/subscriptions/test-sub"
    )

def test_parse_pubsub_push():
    req = _build_push_request(VALIDATION_FAILURE_ENTRY)
    entry = parse_pubsub_push(req)
    assert entry.logName == "projects/test-project/logs/batch_task_logs"
    assert entry.jsonPayload.import_name == "WorldDevelopmentIndicators"
    assert entry.jsonPayload.status == "FAILURE"


def test_to_failure_event_success():
    req = _build_push_request(VALIDATION_FAILURE_ENTRY)
    entry = parse_pubsub_push(req)
    event = to_failure_event(entry)
    
    assert event.job_id == "worlddevelopmentin-047f6f5d-f21d-4b630"
    assert event.import_name == "WorldDevelopmentIndicators"
    assert event.stage_name == "VALIDATION"
    assert event.status == "FAILURE"

def test_to_failure_event_skips_init():
    req = _build_push_request(INIT_ENTRY)
    entry = parse_pubsub_push(req)
    
    with pytest.raises(NotAFailureEvent):
        to_failure_event(entry)

def test_to_failure_event_fallback_import_name():
    # Construct a log payload missing the explicit import_name field, but containing it in message
    custom_entry = dict(VALIDATION_FAILURE_ENTRY)
    custom_entry["jsonPayload"] = {
        "log_type": "auto-import-job-status",
        "status": "FAILURE",
        "message": "Import: USCensusPEP_By_Sex_Race, stage: VALIDATION, status: FAILURE",
        "latency_secs": 87,
        "data_bytes": 21756466,
        "stage_name": "VALIDATION"
    }
    
    req = _build_push_request(custom_entry)
    entry = parse_pubsub_push(req)
    event = to_failure_event(entry)
    
    assert event.import_name == "USCensusPEP_By_Sex_Race"
    assert event.status == "FAILURE"

