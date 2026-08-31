import os
import csv
import logging
from typing import Dict, Any, List, Set
from .validation_reader import fetch_deleted_nodes_sample
from .historical_archives import HISTORICAL_ARCHIVES_DIR
from ..database import db

log = logging.getLogger(__name__)

def clean_dcid(val: str) -> str:
    if not val:
        return ""
    val = val.strip()
    if val.startswith("dcid:"):
        return val[5:]
    return val

def verify_remediation_union(issue_id: str) -> Dict[str, Any]:
    # 1. Fetch issue metadata
    all_issues = db.get_all_results()
    issue = next((item for item in all_issues if item.get('issue_id') == issue_id or item.get('job_id') == issue_id), None)
    if not issue:
        raise ValueError(f"Issue {issue_id} not found in database.")

    latest_run_folder = issue.get("latest_run_folder")
    if not latest_run_folder:
        return {
            "success": False,
            "message": "No run folder associated with this ingestion.",
            "total_deletions": 0,
            "archived_count": 0,
            "remaining_deletions": 0,
            "remaining_samples": []
        }

    run_name = latest_run_folder.strip("/").split("/")[-1]
    
    # 2. Fetch ALL deleted nodes reported by differ (up to 500,000 for full coverage)
    log.info(f"Loading full differ deletions for verification of issue {issue_id}...")
    deleted_nodes = fetch_deleted_nodes_sample(latest_run_folder, limit=500000)
    if not deleted_nodes:
        return {
            "success": True,
            "message": "No deletions reported for this run. Verification skipped.",
            "total_deletions": 0,
            "archived_count": 0,
            "remaining_deletions": 0,
            "remaining_samples": []
        }

    # 3. Locate the generated local historical CSV archive
    archive_dir = os.path.join(HISTORICAL_ARCHIVES_DIR, issue_id, run_name)
    archive_csv = os.path.join(archive_dir, "historical_archive.csv")
    if not os.path.exists(archive_csv):
        return {
            "success": False,
            "message": "Historical archive CSV file not found. Please generate the historical files first.",
            "total_deletions": len(deleted_nodes),
            "archived_count": 0,
            "remaining_deletions": len(deleted_nodes),
            "remaining_samples": []
        }

    # 4. Extract keys from generated archive CSV
    archive_keys: Set[tuple] = set()
    try:
        with open(archive_csv, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader, None) # Skip header ["Place", "Date", "Variable", "Value"]
            for row in reader:
                if len(row) >= 3:
                    place = clean_dcid(row[0])
                    date = row[1].strip()
                    var = clean_dcid(row[2])
                    archive_keys.add((place, date, var))
    except Exception as e:
        log.error(f"Failed to parse generated CSV archive for verification: {e}")
        return {
            "success": False,
            "message": f"Failed to read historical archive CSV: {str(e)}",
            "total_deletions": len(deleted_nodes),
            "archived_count": 0,
            "remaining_deletions": len(deleted_nodes),
            "remaining_samples": []
        }

    # 5. Extract keys from validation deletions (D)
    deletion_keys: List[tuple] = []
    for node in deleted_nodes:
        place = clean_dcid(node.get("observationAbout", ""))
        date = node.get("observationDate", "").strip()
        var = clean_dcid(node.get("variableMeasured", ""))
        if place and date and var:
            deletion_keys.append((place, date, var))

    # 6. Calculate difference: D - A
    remaining_deletions = []
    for key in deletion_keys:
        if key not in archive_keys:
            remaining_deletions.append(key)

    total_deletions = len(deletion_keys)
    archived_count = len(archive_keys)
    remaining_count = len(remaining_deletions)

    log.info(f"Verification completed. Total diff deletions: {total_deletions}, Archive rows: {archived_count}, Unrestored: {remaining_count}")

    # format remaining samples nicely for UI display
    samples = []
    for k in remaining_deletions[:10]:
        samples.append(f"Place: {k[0]}, Date: {k[1]}, Variable: {k[2]}")

    return {
        "success": remaining_count == 0,
        "message": "Verification completed successfully." if remaining_count == 0 else f"Remediation is incomplete. {remaining_count} observations are still missing.",
        "total_deletions": total_deletions,
        "archived_count": archived_count,
        "remaining_deletions": remaining_count,
        "remaining_samples": samples
    }
