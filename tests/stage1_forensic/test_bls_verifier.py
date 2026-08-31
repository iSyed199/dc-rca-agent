import json
import pytest
from unittest.mock import MagicMock, patch
from dc_rca_agent.stage1_forensic.verifiers.bls import BlsVerifier

@patch("urllib.request.urlopen")
def test_bls_verifier_exists(mock_urlopen):
    # Mock BLS response returning series with year data (mismatch!)
    mock_response = MagicMock()
    mock_response.getcode.return_value = 200
    mock_response.read.return_value = json.dumps({
        "status": "REQUEST_SUCCEEDED",
        "Results": {
            "series": [{
                "seriesID": "CUUR0000SA0",
                "data": [{
                    "year": "2026",
                    "period": "M05",
                    "value": "335.123"
                }]
            }]
        }
    }).encode("utf-8")
    mock_urlopen.return_value.__enter__.return_value = mock_response

    verifier = BlsVerifier()
    nodes = [{
        "observationAbout": "country/USA",
        "observationDate": "2026",
        "variableMeasured": "bls/CUUR0000SA0"
    }]
    
    results = verifier.verify_deletions(nodes)
    assert len(results) == 1
    assert results[0]["place"] == "country/USA"
    assert results[0]["year"] == "2026"
    assert results[0]["variable"] == "CUUR0000SA0"
    assert results[0]["value"] == "335.123"
    assert results[0]["status"] == "EXISTS_UPSTREAM"

@patch("urllib.request.urlopen")
def test_bls_verifier_deleted(mock_urlopen):
    # Mock BLS returning data points for other years but not our target year (confirmed deleted!)
    mock_response = MagicMock()
    mock_response.getcode.return_value = 200
    mock_response.read.return_value = json.dumps({
        "status": "REQUEST_SUCCEEDED",
        "Results": {
            "series": [{
                "seriesID": "CUUR0000SA0",
                "data": [{
                    "year": "2025",
                    "period": "M05",
                    "value": "320.100"
                }]
            }]
        }
    }).encode("utf-8")
    mock_urlopen.return_value.__enter__.return_value = mock_response

    verifier = BlsVerifier()
    nodes = [{
        "observationAbout": "country/USA",
        "observationDate": "2026",
        "variableMeasured": "bls/CUUR0000SA0"
    }]
    
    results = verifier.verify_deletions(nodes)
    assert len(results) == 1
    assert results[0]["status"] == "CONFIRMED_DELETED"
