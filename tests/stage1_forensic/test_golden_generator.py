import io
import pytest
from unittest.mock import MagicMock, patch
from dc_rca_agent.stage1_forensic.golden_generator import generate_goldens_in_gcs

@patch("dc_rca_agent.stage1_forensic.golden_generator.storage.Client")
def test_generate_goldens_in_gcs(mock_storage_client_cls):
    # Setup mocks
    mock_client = MagicMock()
    mock_storage_client_cls.return_value = mock_client
    
    mock_bucket = MagicMock()
    mock_client.bucket.return_value = mock_bucket
    
    # 1. Mock Reference Filter Files
    mock_nl_blob = MagicMock()
    mock_nl_blob.download_as_text.return_value = "Count_Student\nCount_Teacher\n"
    
    mock_places_blob = MagicMock()
    mock_places_blob.download_as_text.return_value = "geoId/01\ngeoId/02\n"
    
    # 2. Mock summary_report.csv
    mock_summary_blob = MagicMock()
    mock_summary_blob.name = "run123/summary_report.csv"
    mock_summary_blob.exists.return_value = True
    mock_summary_text = (
        "StatVar,NumPlaces,MinDate,MeasurementMethods,Units,ScalingFactors,observationPeriods,UnusedCol\n"
        "Count_Student,100,2020,CensusMethod,USD,1,P1Y,DummyValue\n"
    )
    mock_summary_blob.download_as_text.return_value = mock_summary_text
    mock_summary_blob.open.return_value.__enter__.side_effect = lambda: io.StringIO(mock_summary_text)
    
    # 3. Mock input observations CSV list
    mock_obs_blob1 = MagicMock()
    mock_obs_blob1.name = "run123/us_demographics.csv"
    mock_csv_bytes = (
        b"sv_name,place,year,observation,unit\n"
        b"Count_Student,geoId/01,2020,50,USD\n"
        b"Count_Student,geoId/99,2020,20,USD\n"
        b"Count_Unknown,geoId/01,2020,10,USD\n"
    )
    mock_obs_blob1.download_as_bytes.side_effect = lambda *args, **kwargs: mock_csv_bytes
    mock_obs_blob1.open.return_value = io.StringIO(mock_csv_bytes.decode('utf-8'))
    
    uploaded_strings = {}
    
    # Client blob dispatch logic
    def get_blob(blob_name):
        if blob_name.endswith("nl_statvars.csv"):
            return mock_nl_blob
        elif blob_name.endswith("top_100k_places.csv"):
            return mock_places_blob
        elif blob_name.endswith("summary_report.csv") and "golden_data" not in blob_name:
            return mock_summary_blob
        elif blob_name.endswith("us_demographics.csv"):
            return mock_obs_blob1
            
        # Capture uploaded content for golden outputs
        g_blob = MagicMock()
        def upload_from_str(s, content_type=None):
            uploaded_strings[blob_name] = s
        g_blob.upload_from_string.side_effect = upload_from_str
        return g_blob
        
    mock_bucket.blob.side_effect = get_blob
    
    # Mock list_blobs to return our summary and observations blobs
    mock_client.list_blobs.return_value = [mock_summary_blob, mock_obs_blob1]
    
    # Run generator
    res = generate_goldens_in_gcs("issue123", "gs://test-bucket/run123")
    
    # Assertions
    assert res["success"] is True
    assert res["scanned_rows"] == 3
    assert res["matched_rows"] == 1
    assert "golden_summary_report.csv" in res["summary_golden"]
    assert "golden_observations.csv" in res["observations_golden"]
    
    # Verify written golden summary content (excludes UnusedCol)
    summary_key = res["summary_golden"].replace("gs://test-bucket/", "")
    summary_golden_content = uploaded_strings[summary_key]
    assert "UnusedCol" not in summary_golden_content
    assert "StatVar,NumPlaces,MinDate,MeasurementMethods,Units,ScalingFactors,observationPeriods" in summary_golden_content
    
    # Verify written golden observations (matches only valid combinations, mapped to target columns)
    obs_key = res["observations_golden"].replace("gs://test-bucket/", "")
    obs_golden_content = uploaded_strings[obs_key]
    assert "variableMeasured,unit,scalingFactor,observationPeriod,measurementMethod,observationAbout,observationDate" in obs_golden_content
    # Count_Student,geoId/01,2020,50,USD matches and gets mapped
    assert "Count_Student,USD,,,,geoId/01,2020" in obs_golden_content
    # Count_Unknown and geoId/99 should not be present
    assert "Count_Unknown" not in obs_golden_content
    assert "geoId/99" not in obs_golden_content

