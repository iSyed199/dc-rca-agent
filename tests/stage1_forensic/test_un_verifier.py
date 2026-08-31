import pytest
from unittest.mock import MagicMock, patch
from dc_rca_agent.stage1_forensic.verifiers.un import UnDataVerifier

@patch("urllib.request.urlopen")
def test_un_verifier_exists(mock_urlopen):
    # Mock UNData response returning valid SDMX XML string
    mock_response = MagicMock()
    mock_response.getcode.return_value = 200
    mock_response.read.return_value = b"<message:Structure>Valid UN response</message:Structure>"
    mock_urlopen.return_value.__enter__.return_value = mock_response

    verifier = UnDataVerifier()
    nodes = [{
        "observationAbout": "country/USA",
        "observationDate": "2020",
        "variableMeasured": "un/Population"
    }]
    
    results = verifier.verify_deletions(nodes)
    assert len(results) == 1
    assert results[0]["place"] == "country/USA"
    assert results[0]["status"] == "EXISTS_UPSTREAM"

@patch("urllib.request.urlopen")
def test_un_verifier_deleted(mock_urlopen):
    # Mock UNData returning non-matching empty/error payload
    mock_response = MagicMock()
    mock_response.getcode.return_value = 200
    mock_response.read.return_value = b"Empty or raw error message"
    mock_urlopen.return_value.__enter__.return_value = mock_response

    verifier = UnDataVerifier()
    nodes = [{
        "observationAbout": "country/USA",
        "observationDate": "2020",
        "variableMeasured": "un/Population"
    }]
    
    results = verifier.verify_deletions(nodes)
    assert len(results) == 1
    assert results[0]["status"] == "CONFIRMED_DELETED"
