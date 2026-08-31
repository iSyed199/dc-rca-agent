import json
import pytest
from unittest.mock import MagicMock, patch
from dc_rca_agent.stage1_forensic.verifiers.oecd import OecdVerifier

@patch("urllib.request.urlopen")
def test_oecd_verifier_exists(mock_urlopen):
    # Mock OECD SDMX-JSON response returning series with observations (mismatch!)
    mock_response = MagicMock()
    mock_response.getcode.return_value = 200
    mock_response.read.return_value = json.dumps({
        "data": {
            "dataSets": [{
                "series": {
                    "0:0:0:0": {
                        "observations": {
                            "0": [78.2, 0]
                        }
                    }
                }
            }]
        }
    }).encode("utf-8")
    mock_urlopen.return_value.__enter__.return_value = mock_response

    verifier = OecdVerifier()
    nodes = [{
        "observationAbout": "country/USA",
        "observationDate": "2020",
        "variableMeasured": "oecd/REGION_DEMOGR"
    }]
    
    results = verifier.verify_deletions(nodes)
    assert len(results) == 1
    assert results[0]["place"] == "USA"
    assert results[0]["year"] == "2020"
    assert results[0]["variable"] == "REGION_DEMOGR"
    assert results[0]["value"] == "78.2"
    assert results[0]["status"] == "EXISTS_UPSTREAM"

@patch("urllib.request.urlopen")
def test_oecd_verifier_deleted(mock_urlopen):
    # Mock OECD returning empty series observations (confirmed deleted!)
    mock_response = MagicMock()
    mock_response.getcode.return_value = 200
    mock_response.read.return_value = json.dumps({
        "data": {
            "dataSets": [{
                "series": {
                    "0:0:0:0": {
                        "observations": {}
                    }
                }
            }]
        }
    }).encode("utf-8")
    mock_urlopen.return_value.__enter__.return_value = mock_response

    verifier = OecdVerifier()
    nodes = [{
        "observationAbout": "country/USA",
        "observationDate": "2020",
        "variableMeasured": "oecd/REGION_DEMOGR"
    }]
    
    results = verifier.verify_deletions(nodes)
    assert len(results) == 1
    assert results[0]["place"] == "USA"
    assert results[0]["status"] == "CONFIRMED_DELETED"
