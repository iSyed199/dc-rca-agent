import pytest
from unittest.mock import MagicMock, patch
from dc_rca_agent.stage1_forensic.verifiers import get_verifier
from dc_rca_agent.stage1_forensic.verifiers.fao import FaoVerifier

def test_get_verifier_routes_fao():
    verifier = get_verifier("FAO_Currency_statvar")
    assert isinstance(verifier, FaoVerifier)
    assert "fao.org" in verifier.portal_url

@patch("urllib.request.urlopen")
def test_fao_verifier_proof_url(mock_urlopen):
    mock_response = MagicMock()
    mock_response.getcode.return_value = 200
    mock_urlopen.return_value.__enter__.return_value = mock_response

    verifier = FaoVerifier()
    nodes = [{
        "observationAbout": "dcid:country/ALB",
        "observationDate": "2021",
        "variableMeasured": "dcid:ExchangeRate_Currency_USD_To_ALL"
    }]
    results = verifier.verify_deletions(nodes)
    assert len(results) == 1
    assert results[0]["query_url"] == "https://www.fao.org/faostat/en/#data/PE"
    assert "google.com" not in results[0]["query_url"]
    assert results[0]["status"] == "EXISTS_UPSTREAM"
