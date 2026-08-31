import json
import pytest
from unittest.mock import MagicMock, patch
from dc_rca_agent.settings import settings
from dc_rca_agent.stage1_forensic.verifiers.fred import FredVerifier

def test_fred_verifier_needs_key():
    # If settings key is empty, it returns NEEDS_API_KEY status
    with patch.object(settings, "fred_api_key", None):
        verifier = FredVerifier()
        nodes = [{
            "observationAbout": "country/USA",
            "observationDate": "2020",
            "variableMeasured": "fed/DGS10"
        }]
        results = verifier.verify_deletions(nodes)
        assert len(results) == 1
        assert results[0]["status"] == "NEEDS_API_KEY"
        assert results[0]["value"] == "Key Required"

@patch("urllib.request.urlopen")
def test_fred_verifier_exists(mock_urlopen):
    # If key is set and data exists, it returns EXISTS_UPSTREAM status
    with patch.object(settings, "fred_api_key", "mock_fred_secret_key"):
        mock_response = MagicMock()
        mock_response.getcode.return_value = 200
        mock_response.read.return_value = json.dumps({
            "observations": [{
                "date": "2020-07-01",
                "value": "0.62"
            }]
        }).encode("utf-8")
        mock_urlopen.return_value.__enter__.return_value = mock_response

        verifier = FredVerifier()
        nodes = [{
            "observationAbout": "country/USA",
            "observationDate": "2020",
            "variableMeasured": "fed/DGS10"
        }]
        
        results = verifier.verify_deletions(nodes)
        assert len(results) == 1
        assert results[0]["place"] == "country/USA"
        assert results[0]["variable"] == "DGS10"
        assert results[0]["value"] == "0.62"
        assert results[0]["status"] == "EXISTS_UPSTREAM"

@patch("urllib.request.urlopen")
def test_fred_verifier_deleted(mock_urlopen):
    # If key is set but API fails/returns nothing, it returns CONFIRMED_DELETED status
    with patch.object(settings, "fred_api_key", "mock_fred_secret_key"):
        mock_urlopen.side_effect = Exception("HTTP 404 Not Found")

        verifier = FredVerifier()
        nodes = [{
            "observationAbout": "country/USA",
            "observationDate": "2020",
            "variableMeasured": "fed/DGS10"
        }]
        
        results = verifier.verify_deletions(nodes)
        assert len(results) == 1
        assert results[0]["status"] == "CONFIRMED_DELETED"
