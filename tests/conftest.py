import os
import shutil
import json

# Force local JSON database for test execution to ensure test isolation
os.environ["DATABASE_TYPE"] = "LOCAL"

# Resolve directories
_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_original_db = os.path.join(_root, "config", "verification_results.json")
_test_db = os.path.join(_root, "config", "test_verification_results.json")

# Prepare test database copy
if os.path.exists(_original_db):
    shutil.copy2(_original_db, _test_db)
else:
    with open(_test_db, "w") as f:
        json.dump([], f)

# Inject test fixtures into test database copy to ensure tests are self-contained
_fixtures = [
  {
    "issue_num": "1",
    "issue_id": "470415967",
    "title": "WorldDevelopmentIndicators",
    "status": "SUCCESS",
    "gcs_path": "gs://test-imports-bucket/scripts/world_bank/wdi/WorldDevelopmentIndicators/",
    "latest_run_folder": "gs://test-imports-bucket/scripts/world_bank/wdi/WorldDevelopmentIndicators/2026_07_07T04_03_12_251188_07_00/",
    "differ_summary_path": "gs://test-imports-bucket/scripts/world_bank/wdi/WorldDevelopmentIndicators/2026_07_07T04_03_12_251188_07_00/input0/validation/differ_summary.json",
    "rca_detail": "Source data change - Inaccurate Place Mapping",
    "differ_summary": {
      "current_version": "/data/scripts/world_bank/wdi/WorldDevelopmentIndicators/2026_07_07T04_03_12_251188_07_00/input0/genmcf/*.mcf",
      "previous_version": "gs://test-imports-bucket/scripts/world_bank/wdi/WorldDevelopmentIndicators/2026_04_07T04_03_20_453090_07_00/WorldBank/genmcf/*.mcf",
      "current_obs_count": 454292,
      "previous_obs_count": 448135,
      "current_schema_count": 0,
      "previous_schema_count": 0,
      "added_obs_count": 7078,
      "deleted_obs_count": 921,
      "modified_obs_count": 77737,
      "added_schema_count": 0,
      "deleted_schema_count": 0,
      "modified_schema_count": 0,
      "obs_diff_count": 85736,
      "schema_diff_count": 0
    },
    "job_id": "worlddevelopmentindicators-1783422002"
  },
  {
    "issue_num": "2",
    "issue_id": "472605922",
    "title": "USCensusPEP_Sex",
    "status": "NO_DIFFER_SUMMARY_FOUND",
    "gcs_path": "gs://test-imports-bucket/scripts/us_census/pep/us_pep_sex/",
    "latest_run_folder": None,
    "rca_detail": "Issue with Differ Tool",
    "job_id": "uscensuspep-sex-1783911629"
  }
]


import json
try:
    with open(_test_db, "r") as f:
        db_data = json.load(f)
    if not isinstance(db_data, list):
        db_data = []
except Exception:
    db_data = []

# Merge fixtures based on issue_id/job_id
for fix in _fixtures:
    exists = any(
        item.get("issue_id") == fix["issue_id"] or item.get("job_id") == fix["job_id"]
        for item in db_data
    )
    if not exists:
        db_data.append(fix)

with open(_test_db, "w") as f:
    json.dump(db_data, f, indent=2)

# Set environment overrides so that settings.py loads test-specific database files
os.environ["RCA_VERIFICATION_RESULTS_PATH"] = _test_db
os.environ["RCA_VERIFICATION_METADATA_PATH"] = os.path.join(_root, "config", "test_verification_metadata.json")

# Ensure database singleton uses the local test database instance
import dc_rca_agent.database as db_module
db_module.db = db_module.get_db()

def pytest_unconfigure(config):
    """
    Cleans up the temporary database copy after all tests finish.
    """
    if os.path.exists(_test_db):
        try:
            os.remove(_test_db)
        except Exception:
            pass
            
    test_metadata = os.path.join(_root, "config", "test_verification_metadata.json")
    if os.path.exists(test_metadata):
        try:
            os.remove(test_metadata)
        except Exception:
            pass
