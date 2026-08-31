import pytest
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock
from dc_rca_agent.settings import settings
from dc_rca_agent.models import FailureEvent
from dc_rca_agent.stage1_forensic.path_resolver import resolve_gcs_run_folder

@patch("subprocess.run")
def test_resolve_gcs_run_folder(mock_run):
    base_prefix = f"gs://{settings.imports_bucket}/statvar_imports/us_bachelors_degree_data/nces_bachelors_degree_by_field_import/"
    mock_run.return_value = MagicMock(
        returncode=0,
        stdout=f"{base_prefix}2026_07_28T02_13_39/\n"
    )

    event = FailureEvent(
        job_id="nces-bachelors-deg-7170a5d0-4f7a-4e270",
        job_uid="nces-bachelors-deg-7170a5d0-4f7a-4e270",
        import_name="NCES_Bachelors_Degree_By_Field_Import",
        stage_name="VALIDATION",
        status="FAILURE",
        message="FAILED: check_deleted_records_percent",
        timestamp=datetime(2026, 7, 28, 2, 13, 39, tzinfo=timezone.utc),
        job_name="nces-bachelors-degree-1785239082"
    )
    
    resolved = resolve_gcs_run_folder(event)
    assert resolved is not None
    assert resolved.startswith(base_prefix)

