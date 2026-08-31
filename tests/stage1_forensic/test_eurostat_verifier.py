import json
import pytest
from unittest.mock import MagicMock, patch
from dc_rca_agent.stage1_forensic.verifiers.eurostat import EurostatVerifier

@patch("urllib.request.urlopen")
def test_eurostat_verifier_exists(mock_urlopen):
    # Mock Eurostat JSON response returning a non-empty values dictionary (mismatch!)
    mock_response = MagicMock()
    mock_response.getcode.return_value = 200
    mock_response.read.return_value = json.dumps({
        "version": "2.0",
        "value": {
            "0": 12500.5
        }
    }).encode("utf-8")
    mock_urlopen.return_value.__enter__.return_value = mock_response

    verifier = EurostatVerifier()
    nodes = [{
        "observationAbout": "country/DNK",
        "observationDate": "2019",
        "variableMeasured": "eurostat/educ_uoe_enrt01"
    }]
    
    results = verifier.verify_deletions(nodes)
    assert len(results) == 1
    assert results[0]["place"] == "DK"
    assert results[0]["year"] == "2019"
    assert results[0]["variable"] == "educ_uoe_enrt01"
    assert results[0]["value"] == "12500.5"
    assert results[0]["status"] == "EXISTS_UPSTREAM"

@patch("urllib.request.urlopen")
def test_eurostat_verifier_deleted(mock_urlopen):
    # Mock Eurostat returning an empty value dictionary (confirmed deleted!)
    mock_response = MagicMock()
    mock_response.getcode.return_value = 200
    mock_response.read.return_value = json.dumps({
        "version": "2.0",
        "value": {}
    }).encode("utf-8")
    mock_urlopen.return_value.__enter__.return_value = mock_response

    verifier = EurostatVerifier()
    nodes = [{
        "observationAbout": "country/BEL",
        "observationDate": "2020",
        "variableMeasured": "eurostat/educ_uoe_enrt01"
    }]
    
    results = verifier.verify_deletions(nodes)
    assert len(results) == 1
    assert results[0]["place"] == "BE"
    assert results[0]["status"] == "CONFIRMED_DELETED"
