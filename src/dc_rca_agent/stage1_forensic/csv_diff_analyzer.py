from __future__ import annotations
import logging
import csv
import subprocess
import json
import io
import re
from typing import Dict, Any, List, Tuple
from ..settings import settings
from ..database import db

log = logging.getLogger(__name__)

def find_root_csv_file(run_path: str) -> str | None:
    """
    Lists files directly under the run_path and returns the most relevant root CSV file path
    containing observation data, inspecting headers if multiple exist.
    """
    try:
        res = subprocess.run([
            settings.gcloud_bin_path,
            "storage", "ls", run_path
        ], capture_output=True, text=True, check=True)
        
        csv_files = []
        lines = res.stdout.strip().split('\n')
        for line in lines:
            path = line.strip()
            # Must end with .csv and NOT contain subfolders like input*/ or provenance/
            if path.endswith(".csv") and not re.search(r'/input\d+/', path) and not any(sub in path for sub in ["/provenance/", "/source_files/", "/validation/", "/counters/", "/golden_data/"]):
                csv_files.append(path)
                
        if not csv_files:
            return None
            
        if len(csv_files) == 1:
            return csv_files[0]
            
        # If there are multiple, look for the one containing a known variable header
        common_var_headers = ["StatisticalVariable", "statisticalVariable", "StatVar", "statVar", "Variable", "variable", "variableMeasured", "sv_name"]
        for path in csv_files:
            try:
                # Read first 500 bytes of the file to check headers
                header_res = subprocess.run([
                    settings.gcloud_bin_path,
                    "storage", "cat", path, "--range=0-500"
                ], capture_output=True, text=True, timeout=5)
                if header_res.returncode == 0:
                    first_line = header_res.stdout.split('\n')[0]
                    headers = [h.strip() for h in first_line.split(',')]
                    if any(h in headers for h in common_var_headers):
                        log.info(f"Selected observations CSV file based on headers: {path}")
                        return path
            except Exception as e:
                log.warning(f"Error inspecting CSV header for {path}: {e}")
                
        # Default fallback to first one if no header matches
        return csv_files[0]
    except Exception as e:
        log.error(f"Error finding root CSV file in run path '{run_path}': {e}")
    return None

def read_csv_summary(csv_gcs_path: str, max_rows: int = 100_000) -> Tuple[List[str], Dict[str, set[str]], bool] | None:
    """
    Streams a CSV file from GCS and returns:
    1. The list of headers.
    2. A dictionary mapping unique variables to their set of coordinate keys.
    3. A boolean indicating if the parsing was truncated.
    """
    try:
        process = subprocess.Popen([
            settings.gcloud_bin_path,
            "storage", "cat", csv_gcs_path
        ], stdout=subprocess.PIPE)
        headers = []
        var_coords = {}
        is_truncated = False
        row_count = 0
        
        # Read header first
        first_chunk = True
        for line in io.TextIOWrapper(process.stdout, encoding="utf-8"):
            row = next(csv.reader([line]))
            if first_chunk:
                headers = [h.strip() for h in row]
                first_chunk = False
                continue
                
            row_count += 1
            if row_count > max_rows:
                is_truncated = True
                process.terminate()
                break
                
            # Find the index of the Statistical Variable measured column
            var_idx = -1
            common_var_headers = ["StatisticalVariable", "statisticalVariable", "StatVar", "statVar", "Variable", "variable", "variableMeasured", "sv_name"]
            for h in common_var_headers:
                if h in headers:
                    var_idx = headers.index(h)
                    break
                    
            val_idx = -1
            common_val_headers = ["Value", "value", "observation", "Observation", "obsValue", "ObsValue"]
            for h in common_val_headers:
                if h in headers:
                    val_idx = headers.index(h)
                    break
                
            if var_idx != -1 and len(row) > var_idx:
                var_name = row[var_idx].strip()
                if var_name.startswith("dcid:"):
                    var_name = var_name.replace("dcid:", "")
                
                coord_parts = [row[i].strip() for i in range(len(row)) if i != val_idx and i != var_idx]
                coord_key = "|".join(coord_parts)
                
                if var_name not in var_coords:
                    var_coords[var_name] = set()
                var_coords[var_name].add(coord_key)
            else:
                # Fallback to generic line count if headers aren't standard
                if "TotalRows" not in var_coords:
                    var_coords["TotalRows"] = set()
                var_coords["TotalRows"].add(str(len(var_coords["TotalRows"])))
                
        process.wait()
        return headers, var_coords, is_truncated
    except Exception as e:
        log.error(f"Failed to read CSV summary from GCS path '{csv_gcs_path}': {e}")
        return None

def compute_csv_regression_diff(issue_id: str) -> Dict[str, Any] | None:
    """
    Resolves current and previous GCS run output CSV files, streams them, and returns schema & row count diffs.
    """
    results = db.get_all_results()
    import_data = next((r for r in results if r.get('issue_id') == issue_id or r.get('job_id') == issue_id or str(r.get('issue_num')) == str(issue_id) or r.get('title') == issue_id), None)
    if not import_data:
        return None

        
    current_run_path = import_data.get('latest_run_folder')
    if not current_run_path:
        return None
        
    # Parent path contains latest_version.txt
    # Current run path matches: gs://bucket/prefix/timestamp/
    match = re.match(r'^(gs://[^/]+/(?:.+/)+)[^/]+/?$', current_run_path)
    if not match:
        return None
        
    parent_path = match.group(1)
    latest_version_txt = parent_path + "latest_version.txt"
    
    try:
        # Read latest_version.txt content
        res = subprocess.run([
            settings.gcloud_bin_path,
            "storage", "cat", latest_version_txt
        ], capture_output=True, text=True, check=True)
        prev_timestamp = res.stdout.strip()
    except Exception as e:
        log.error(f"Could not read previous version timestamp from '{latest_version_txt}': {e}")
        return None
        
    prev_run_path = parent_path + prev_timestamp + "/"
    log.info(f"Resolved previous run path: {prev_run_path}")
    
    current_csv = find_root_csv_file(current_run_path)
    prev_csv = find_root_csv_file(prev_run_path)
    
    if not current_csv or not prev_csv:
        log.warning(f"Could not locate output CSV files for diff: current={current_csv}, prev={prev_csv}")
        return None
        
    log.info(f"Diffing CSVs: current={current_csv}, prev={prev_csv}")
    
    current_data = read_csv_summary(current_csv)
    prev_data = read_csv_summary(prev_csv)
    
    if not current_data or not prev_data:
        return None
        
    curr_headers, curr_counts, curr_trunc = current_data
    prev_headers, prev_counts, prev_trunc = prev_data
    
    # Calculate schema changes
    added_cols = [c for c in curr_headers if c not in prev_headers]
    removed_cols = [c for c in prev_headers if c not in curr_headers]
    
    # Calculate row count differences per variable
    all_vars = set(curr_counts.keys()).union(set(prev_counts.keys()))
    variable_diff = []
    
    for var in all_vars:
        curr_set = curr_counts.get(var, set())
        prev_set = prev_counts.get(var, set())
        
        c_count = len(curr_set)
        p_count = len(prev_set)
        
        added = len(curr_set - prev_set)
        deleted = len(prev_set - curr_set)
        diff = c_count - p_count
        
        if added > 0 or deleted > 0:
            variable_diff.append({
                "variable": var,
                "previous_count": p_count,
                "current_count": c_count,
                "diff": diff,
                "added_count": added,
                "deleted_count": deleted
            })
            
    # Sort by total modifications (additions + deletions) descending
    variable_diff.sort(key=lambda x: (x["added_count"] + x["deleted_count"]), reverse=True)
    
    return {
        "previous_version": prev_timestamp,
        "current_version": current_run_path.rstrip('/').split('/')[-1],
        "previous_columns": prev_headers,
        "current_columns": curr_headers,
        "is_truncated": curr_trunc or prev_trunc,
        "schema_diff": {
            "added_columns": added_cols,
            "removed_columns": removed_cols
        },
        "variable_row_diff": variable_diff
    }
