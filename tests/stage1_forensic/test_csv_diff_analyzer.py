from __future__ import annotations
import pytest
from unittest.mock import patch, MagicMock
from dc_rca_agent.stage1_forensic.csv_diff_analyzer import compute_csv_regression_diff, find_root_csv_file

@patch("subprocess.run")
def test_find_root_csv_file(mock_run):
    mock_run.return_value = MagicMock(
        returncode=0,
        stdout="gs://test-bucket/scripts/world_bank/wdi/2026_07_06T01_33_17/WorldBank.csv\n"
    )
    run_path = "gs://test-bucket/scripts/world_bank/wdi/2026_07_06T01_33_17/"
    csv_file = find_root_csv_file(run_path)
    assert csv_file is not None
    assert csv_file.endswith("WorldBank.csv")

@patch("dc_rca_agent.stage1_forensic.csv_diff_analyzer.read_csv_summary")
@patch("dc_rca_agent.stage1_forensic.csv_diff_analyzer.find_root_csv_file")
@patch("subprocess.run")
def test_compute_csv_regression_diff(mock_run, mock_find_csv, mock_read_summary):
    # Mock reading latest_version.txt
    mock_run.return_value = MagicMock(
        returncode=0,
        stdout="2026_04_07T04_03_20_453090_07_00\n"
    )
    mock_find_csv.side_effect = [
        "gs://test-bucket/scripts/world_bank/wdi/2026_07_07T04_03_12/WorldBank.csv",
        "gs://test-bucket/scripts/world_bank/wdi/2026_04_07T04_03_20/WorldBank.csv"
    ]
    mock_read_summary.side_effect = [
        # (headers, var_coords, is_truncated)
        (["Variable", "Value", "Date"], {"GDP": {"coord1", "coord2"}}, False),
        (["Variable", "Value", "Date"], {"GDP": {"coord1"}}, False)
    ]

    diff = compute_csv_regression_diff("470415967")
    assert diff is not None
    assert "previous_version" in diff
    assert "current_version" in diff
    assert "schema_diff" in diff
    assert "variable_row_diff" in diff
    
    # Assert headers are parsed correctly
    assert len(diff["current_columns"]) > 0
    # Assert some differences exist between runs
    assert len(diff["variable_row_diff"]) > 0
    first_diff = diff["variable_row_diff"][0]
    assert first_diff["variable"] == "GDP"
    assert first_diff["previous_count"] == 1
    assert first_diff["current_count"] == 2
    assert first_diff["diff"] == 1

