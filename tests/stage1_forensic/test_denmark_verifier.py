import json
import pytest
from unittest.mock import MagicMock, patch
from dc_rca_agent.stage1_forensic.verifiers.denmark import DenmarkVerifier

@patch("urllib.request.urlopen")
def test_denmark_verifier_exists(mock_urlopen):
    # Mock Denmark Statistics API returning observations in JSON-stat format
    mock_response = MagicMock()
    mock_response.getcode.return_value = 200
    mock_response.read.return_value = json.dumps({
        "dataset": {
            "value": [5822763]
        }
    }).encode("utf-8")
    mock_urlopen.return_value.__enter__.return_value = mock_response

    verifier = DenmarkVerifier()
    nodes = [{
        "observationAbout": "country/DNK",
        "observationDate": "2020",
        "variableMeasured": "denmark/Population"
    }]
    
    results = verifier.verify_deletions(nodes)
    assert len(results) == 1
    assert results[0]["place"] == "Denmark"
    assert results[0]["year"] == "2020"
    assert results[0]["variable"] == "Population"
    assert results[0]["value"] == "5822763"
    assert results[0]["status"] == "EXISTS_UPSTREAM"

@patch("urllib.request.urlopen")
def test_denmark_verifier_deleted(mock_urlopen):
    # Mock Denmark Statistics API returning empty observations list
    mock_response = MagicMock()
    mock_response.getcode.return_value = 200
    mock_response.read.return_value = json.dumps({
        "dataset": {
            "value": []
        }
    }).encode("utf-8")
    mock_urlopen.return_value.__enter__.return_value = mock_response

    verifier = DenmarkVerifier()
    nodes = [{
        "observationAbout": "country/DNK",
        "observationDate": "2020",
        "variableMeasured": "denmark/Population"
    }]
    
    results = verifier.verify_deletions(nodes)
    assert len(results) == 1
    assert results[0]["status"] == "CONFIRMED_DELETED"
