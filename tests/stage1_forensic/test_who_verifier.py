import json
import pytest
from unittest.mock import MagicMock, patch
from dc_rca_agent.stage1_forensic.verifiers.who import WhoVerifier

@patch("urllib.request.urlopen")
def test_who_verifier_exists(mock_urlopen):
    # Mock WHO response returning target year and place observation
    mock_response = MagicMock()
    mock_response.getcode.return_value = 200
    mock_response.read.return_value = json.dumps({
        "value": [{
            "TimeDim": 2020,
            "SpatialDim": "USA",
            "NumericValue": 78.54
        }]
    }).encode("utf-8")
    mock_urlopen.return_value.__enter__.return_value = mock_response

    verifier = WhoVerifier()
    nodes = [{
        "observationAbout": "country/USA",
        "observationDate": "2020",
        "variableMeasured": "who/WHOSIS_000001"
    }]
    
    results = verifier.verify_deletions(nodes)
    assert len(results) == 1
    assert results[0]["place"] == "USA"
    assert results[0]["year"] == "2020"
    assert results[0]["variable"] == "WHOSIS_000001"
    assert results[0]["value"] == "78.54"
    assert results[0]["status"] == "EXISTS_UPSTREAM"

@patch("urllib.request.urlopen")
def test_who_verifier_deleted(mock_urlopen):
    # Mock WHO response with no matching data points
    mock_response = MagicMock()
    mock_response.getcode.return_value = 200
    mock_response.read.return_value = json.dumps({
        "value": [{
            "TimeDim": 2019,
            "SpatialDim": "USA",
            "NumericValue": 77.10
        }]
    }).encode("utf-8")
    mock_urlopen.return_value.__enter__.return_value = mock_response

    verifier = WhoVerifier()
    nodes = [{
        "observationAbout": "country/USA",
        "observationDate": "2020",
        "variableMeasured": "who/WHOSIS_000001"
    }]
    
    results = verifier.verify_deletions(nodes)
    assert len(results) == 1
    assert results[0]["status"] == "CONFIRMED_DELETED"
