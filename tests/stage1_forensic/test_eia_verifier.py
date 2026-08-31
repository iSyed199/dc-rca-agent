import json
import pytest
from unittest.mock import MagicMock, patch
from dc_rca_agent.settings import settings
from dc_rca_agent.stage1_forensic.verifiers.eia import EiaVerifier

def test_eia_verifier_needs_key():
    # If settings key is empty, it returns NEEDS_API_KEY status
    with patch.object(settings, "eia_api_key", None):
        verifier = EiaVerifier()
        nodes = [{
            "observationAbout": "geoId/06",
            "observationDate": "2021",
            "variableMeasured": "eia/seds_TEPRB"
        }]
        results = verifier.verify_deletions(nodes)
        assert len(results) == 1
        assert results[0]["status"] == "NEEDS_API_KEY"
        assert results[0]["value"] == "Key Required"

@patch("urllib.request.urlopen")
def test_eia_verifier_exists(mock_urlopen):
    # If key is set and data exists, it returns EXISTS_UPSTREAM status
    with patch.object(settings, "eia_api_key", "mock_eia_secret_key"):
        mock_response = MagicMock()
        mock_response.getcode.return_value = 200
        mock_response.read.return_value = json.dumps({
            "response": {
                "data": [{
                    "period": "2021",
                    "stateId": "CA",
                    "msn": "TEPRB",
                    "value": "12345.67"
                }]
            }
        }).encode("utf-8")
        mock_urlopen.return_value.__enter__.return_value = mock_response

        verifier = EiaVerifier()
        nodes = [{
            "observationAbout": "geoId/06",
            "observationDate": "2021",
            "variableMeasured": "eia/seds_TEPRB"
        }]
        
        results = verifier.verify_deletions(nodes)
        assert len(results) == 1
        assert results[0]["place"] == "CA"
        assert results[0]["variable"] == "TEPRB"
        assert results[0]["value"] == "12345.67"
        assert results[0]["status"] == "EXISTS_UPSTREAM"

@patch("urllib.request.urlopen")
def test_eia_verifier_deleted(mock_urlopen):
    # If key is set but API fails/returns nothing, it returns CONFIRMED_DELETED status
    with patch.object(settings, "eia_api_key", "mock_eia_secret_key"):
        mock_urlopen.side_effect = Exception("HTTP 404 Not Found")

        verifier = EiaVerifier()
        nodes = [{
            "observationAbout": "geoId/06",
            "observationDate": "2021",
            "variableMeasured": "eia/seds_TEPRB"
        }]
        
        results = verifier.verify_deletions(nodes)
        assert len(results) == 1
        assert results[0]["status"] == "CONFIRMED_DELETED"
