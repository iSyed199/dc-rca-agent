import os
import csv
import io
import logging
import subprocess
from typing import Dict, Any, List
from .validation_reader import fetch_deleted_nodes_sample
from ..database import db
from ..settings import settings

log = logging.getLogger(__name__)

HISTORICAL_ARCHIVES_DIR = settings.historical_archives_path

def upload_archive_to_gcs(issue_id: str, run_name: str, local_path: str, filename: str) -> str:
    gcs_target = f"gs://{settings.imports_bucket}/historical_archives/{issue_id}/{run_name}/{filename}"
    try:
        res = subprocess.run([
            settings.gcloud_bin_path,
            "storage", "cp",
            local_path,
            gcs_target
        ], capture_output=True, text=True)
        if res.returncode == 0:
            log.info(f"Successfully uploaded {filename} to {gcs_target}")
            return gcs_target
        else:
            log.error(f"Failed to upload to GCS: {res.stderr}")
    except Exception as e:
        log.exception(f"Error executing gcloud storage cp: {e}")
    return ""

def generate_historical_archive(issue_id: str) -> Dict[str, Any]:
    """
    Isolates deleted data nodes from validation differ files, and packages them
    into a standard historical CSV archive and Template MCF mapping file.
    """
    # 1. Fetch issue metadata from DB
    all_issues = db.get_all_results()
    issue = next((item for item in all_issues if item.get('issue_id') == issue_id or item.get('job_id') == issue_id or str(item.get('issue_num')) == str(issue_id) or item.get('title') == issue_id), None)
    if not issue:
        raise ValueError(f"Issue {issue_id} not found in database.")


    # 2. Parse deleted nodes from GCS logs/validation files
    log.info(f"Reading validation deletions for packaging issue {issue_id}...")
    latest_run_folder = issue.get("latest_run_folder")
    if not latest_run_folder:
        log.warning(f"No run folders found for issue {issue_id}. Package generation skipped.")
        return {
            "csv_path": "",
            "tmcf_path": "",
            "csv_content": "",
            "tmcf_content": "",
            "count": 0
        }
        
    run_name = latest_run_folder.strip("/").split("/")[-1]
    deleted_nodes = fetch_deleted_nodes_sample(latest_run_folder, limit=500000)
    
    if not deleted_nodes:
        log.warning(f"No deleted nodes found for issue {issue_id}. Package generation skipped.")
        return {
            "csv_path": "",
            "tmcf_path": "",
            "csv_content": "",
            "tmcf_content": "",
            "count": 0
        }

    # 3. Create target directory
    target_dir = os.path.join(HISTORICAL_ARCHIVES_DIR, issue_id, run_name)
    os.makedirs(target_dir, exist_ok=True)

    # 4. Generate CSV archive
    csv_filename = "historical_archive.csv"
    csv_path = os.path.join(target_dir, csv_filename)
    
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    # Write header
    writer.writerow(["Place", "Date", "Variable", "Value"])
    
    row_count = 0
    for node in deleted_nodes:
        place = node.get("observationAbout", "")
        date = node.get("observationDate", "")
        var = node.get("variableMeasured", "")
        
        # Resolve deleted value from y (previous) or direct val
        val = node.get("value")
        if val is None or val == "":
            # Try combined x/y values
            val = node.get("value_combined_y") or node.get("value_combined_x", "0.0")
            
        writer.writerow([place, date, var, val])
        row_count += 1

    csv_content = csv_buffer.getvalue()
    with open(csv_path, "w", encoding="utf-8") as f:
        f.write(csv_content)

    # 5. Generate Template MCF (TMCF) mapping file
    tmcf_filename = "historical_archive.tmcf"
    tmcf_path = os.path.join(target_dir, tmcf_filename)
    
    tmcf_content = (
        "Node: About\n"
        "typeOf: Observation\n"
        "observationAbout: C:HistoricalData->Place\n"
        "observationDate: C:HistoricalData->Date\n"
        "variableMeasured: C:HistoricalData->Variable\n"
        "value: C:HistoricalData->Value\n"
    )
    
    with open(tmcf_path, "w", encoding="utf-8") as f:
        f.write(tmcf_content)

    log.info(f"Packaged {row_count} historical observations into: {csv_path}")
    
    gcs_csv_path = upload_archive_to_gcs(issue_id, run_name, csv_path, csv_filename)
    gcs_tmcf_path = upload_archive_to_gcs(issue_id, run_name, tmcf_path, tmcf_filename)

    return {
        "csv_path": csv_path,
        "tmcf_path": tmcf_path,
        "gcs_csv_path": gcs_csv_path,
        "gcs_tmcf_path": gcs_tmcf_path,
        "csv_content": csv_content,
        "tmcf_content": tmcf_content,
        "count": row_count,
        "run_name": run_name
    }
