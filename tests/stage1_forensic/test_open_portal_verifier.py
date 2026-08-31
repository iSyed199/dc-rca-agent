import json
import pytest
from unittest.mock import MagicMock, patch
from dc_rca_agent.settings import settings
from dc_rca_agent.stage1_forensic.verifiers.open_portal import OpenPortalVerifier

@patch("urllib.request.urlopen")
def test_open_portal_cdc_exists(mock_urlopen):
    # Mock CDC SODA response with dummy list
    mock_response = MagicMock()
    mock_response.getcode.return_value = 200
    mock_response.read.return_value = b"[{\"data_value\": \"12.3\"}]"
    mock_urlopen.return_value.__enter__.return_value = mock_response

    verifier = OpenPortalVerifier("cdc")
    nodes = [{
        "observationAbout": "geoId/06075",
        "observationDate": "2020",
        "variableMeasured": "cdc/BPHIGH"
    }]
    results = verifier.verify_deletions(nodes)
    assert len(results) == 1
    assert results[0]["status"] == "EXISTS_UPSTREAM"
    assert results[0]["value"] == "Active"

@patch("urllib.request.urlopen")
def test_open_portal_bea_needs_key(mock_urlopen):
    # Without BEA key set, it flags NEEDS_API_KEY
    verifier = OpenPortalVerifier("bea")
    nodes = [{
        "observationAbout": "geoId/06",
        "observationDate": "2021",
        "variableMeasured": "bea/GDP"
    }]
    results = verifier.verify_deletions(nodes)
    assert len(results) == 1
    assert results[0]["status"] == "NEEDS_API_KEY"

