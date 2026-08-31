# Mock Cloud Logging entries for Data Commons automated import jobs

INIT_ENTRY = {
  "logName": "projects/test-project/logs/batch_task_logs",
  "resource": {
    "type": "batch.googleapis.com/Job",
    "labels": {
      "resource_container": "test-project",
      "location": "us-central1-c",
      "job_id": "worlddevelopmentin-047f6f5d-f21d-4b630"
    }
  },
  "jsonPayload": {
    "log_type": "auto-import-job-status",
    "status": "SUCCESS",
    "message": "Import: WorldDevelopmentIndicators stage: INIT status: SUCCESS",
    "import_name": "WorldDevelopmentIndicators",
    "latency_secs": 0,
    "data_bytes": 0,
    "stage_name": "INIT"
  },
  "timestamp": "2026-06-30T11:03:40.767165Z",
  "labels": {
    "task_group_name": "projects/123456789012/locations/us-central1/jobs/worlddevelopmentindicators-1782817202/taskGroups/group0",
    "job_uid": "worlddevelopmentin-047f6f5d-f21d-4b630"
  }
}

VALIDATION_FAILURE_ENTRY = {
  "logName": "projects/test-project/logs/batch_task_logs",
  "resource": {
    "type": "batch.googleapis.com/Job",
    "labels": {
      "resource_container": "test-project",
      "location": "us-central1-c",
      "job_id": "worlddevelopmentin-047f6f5d-f21d-4b630"
    }
  },
  "jsonPayload": {
    "log_type": "auto-import-job-status",
    "status": "FAILURE",
    "message": "FAILED: check_deleted_records_percent",
    "import_name": "WorldDevelopmentIndicators",
    "latency_secs": 87,
    "data_bytes": 21756466,
    "stage_name": "VALIDATION"
  },
  "timestamp": "2026-06-30T12:04:43.491490Z",
  "labels": {
    "task_group_name": "projects/123456789012/locations/us-central1/jobs/worlddevelopmentindicators-1782817202/taskGroups/group0",
    "job_uid": "worlddevelopmentin-047f6f5d-f21d-4b630"
  }
}

