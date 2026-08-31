from unittest.mock import patch
from dc_rca_agent.main import get_verification_results, get_deletions_sample, generate_archives_package, get_csv_regression_diff

def test_api_verification_results_direct():
    results = get_verification_results()
    assert len(results) > 0
    first = results[0]
    assert "title" in first
    assert "status" in first

@patch("dc_rca_agent.main.fetch_deleted_nodes_sample")
def test_api_deletions_sample_world_bank_direct(mock_fetch):
    mock_fetch.return_value = [
        {
            "variableMeasured": "dcid:Amount_EconomicActivity_GrossDomesticProduction_Nominal",
            "observationAbout": "dcid:country/USA",
            "observationDate": "2020",
            "value": "21000000"
        }
    ]
    # Call the controller function directly with the unique WDI issue ID (470415967)
    data = get_deletions_sample("470415967")
    assert "sample_mcf" in data
    assert "Node:" in data["sample_mcf"]
    assert "variableMeasured:" in data["sample_mcf"]
    assert "aggregated_svs" in data
    assert len(data["aggregated_svs"]) > 0
    assert "variable" in data["aggregated_svs"][0]
    assert "count" in data["aggregated_svs"][0]
    assert "has_nl_impact" in data["aggregated_svs"][0]

def test_api_deletions_sample_failed_job_direct():
    # Call the controller function directly for a failed job issue ID (e.g. 472605922)
    data = get_deletions_sample("472605922")
    assert data["sample_mcf"] == ""
    assert data["aggregated_svs"] == []

@patch("dc_rca_agent.main.generate_historical_archive")
def test_api_archives_package_direct(mock_archive):
    mock_archive.return_value = {
        "csv_path": "/tmp/test/historical_archive.csv",
        "tmcf_path": "/tmp/test/historical_archive.tmcf",
        "gcs_csv_path": "gs://test-bucket/historical_archive.csv",
        "gcs_tmcf_path": "gs://test-bucket/historical_archive.tmcf",
        "csv_content": "Place,Date,Variable,Value\ncountry/USA,2020,GDP,100",
        "tmcf_content": "Node: About\n",
        "count": 1,
        "run_name": "2026_07_07T04_03_12"
    }
    # Call the controller directly for the WDI deletions issue
    package = generate_archives_package("470415967")
    assert package["status"] == "success"
    assert package["count"] > 0
    assert "csv_path" in package
    assert len(package["commands"]) == 4
    assert "gcloud storage cp" in package["commands"][0]
    assert "git commit" in package["commands"][3]

@patch("dc_rca_agent.main.compute_csv_regression_diff")
def test_api_csv_regression_diff_direct(mock_diff):
    mock_diff.return_value = {
        "previous_version": "2026_04_07T04_03_20",
        "current_version": "2026_07_07T04_03_12",
        "schema_diff": {"added_columns": [], "removed_columns": []},
        "variable_row_diff": [{"variable": "GDP", "diff": 10}]
    }
    data = get_csv_regression_diff("470415967")
    assert data is not None
    assert "previous_version" in data
    assert "current_version" in data
    assert "schema_diff" in data
    assert isinstance(data["variable_row_diff"], list)


@patch("dc_rca_agent.main.run_stages")
def test_api_pubsub_endpoint_validation_failure(mock_run_stages):
    from fastapi.testclient import TestClient
    from dc_rca_agent.main import app
    import base64
    import json
    from tests.fixtures.log_events import VALIDATION_FAILURE_ENTRY

    client = TestClient(app)

    # Encode payload
    raw_payload_str = json.dumps(VALIDATION_FAILURE_ENTRY)
    encoded_payload = base64.b64encode(raw_payload_str.encode('utf-8')).decode('utf-8')

    payload = {
        "message": {
            "messageId": "12345",
            "publishTime": "2026-06-30T12:04:43.491490Z",
            "data": encoded_payload
        },
        "subscription": "projects/test/subscriptions/test-sub"
    }

    # Post to /pubsub
    response = client.post("/pubsub", json=payload)
    assert response.status_code == 202
    assert response.json() == {"status": "accepted"}


def test_api_pubsub_endpoint_skipped():
    from fastapi.testclient import TestClient
    from dc_rca_agent.main import app
    import base64
    import json
    from tests.fixtures.log_events import INIT_ENTRY

    client = TestClient(app)

    # Encode payload
    raw_payload_str = json.dumps(INIT_ENTRY)
    encoded_payload = base64.b64encode(raw_payload_str.encode('utf-8')).decode('utf-8')

    payload = {
        "message": {
            "messageId": "12345",
            "publishTime": "2026-06-30T11:03:40.767165Z",
            "data": encoded_payload
        },
        "subscription": "projects/test/subscriptions/test-sub"
    }

    # Post to /pubsub
    response = client.post("/pubsub", json=payload)
    assert response.status_code == 202
    assert response.json() == {"status": "skipped"}

