from __future__ import annotations
import logging
import json
import csv
import subprocess
from google.cloud import bigquery
from typing import Dict, Any, List, Tuple
from ..settings import settings

log = logging.getLogger(__name__)

from google.cloud import storage
import re

def _get_run_blobs(run_path: str, max_results: int = 500) -> List[Any]:
    """Dynamically lists all GCS blobs under the run folder prefix."""
    if not run_path or not run_path.startswith("gs://"):
        return []
    try:
        clean_path = re.sub(r'/input\d+/?$', '', run_path.strip()).rstrip('/') + '/'
        bucket_name = clean_path.replace("gs://", "").split("/")[0]
        prefix = "/".join(clean_path.replace("gs://", "").split("/")[1:])
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        return list(bucket.list_blobs(prefix=prefix, max_results=max_results))
    except Exception as e:
        log.warning(f"Error listing blobs for run {run_path}: {e}")
        return []

def fetch_differ_summary(run_path: str) -> Dict[str, Any] | None:
    blobs = _get_run_blobs(run_path)
    differ_blobs = [b for b in blobs if b.name.endswith("differ_summary.json")]
    
    # Fallback to direct path check if blob listing empty
    if not differ_blobs and run_path and run_path.startswith("gs://"):
        clean_path = re.sub(r'/input\d+/?$', '', run_path.strip()).rstrip('/')
        possible_paths = [
            clean_path + "/input0/validation/differ_summary.json",
            clean_path + "/validation/differ_summary.json",
            clean_path + "/input0/genmcf/differ_summary.json"
        ]
        for path in possible_paths:
            try:
                res = subprocess.run([settings.gcloud_bin_path, "storage", "cat", path], capture_output=True, text=True)
                if res.returncode == 0 and res.stdout.strip():
                    return json.loads(res.stdout.strip())
            except Exception:
                pass
        return None

    summaries = []
    for blob in differ_blobs:
        try:
            content = blob.download_as_text()
            if content.strip():
                summaries.append(json.loads(content.strip()))
        except Exception as e:
            log.warning(f"Failed to read differ summary blob {blob.name}: {e}")
            
    if not summaries:
        return None
        
    if len(summaries) == 1:
        return summaries[0]
        
    # Aggregate stats across all multi-input partitions
    return {
        "current_obs_count": sum(s.get("current_obs_count", 0) for s in summaries),
        "previous_obs_count": sum(s.get("previous_obs_count", 0) for s in summaries),
        "added_obs_count": sum(s.get("added_obs_count", 0) for s in summaries),
        "deleted_obs_count": sum(s.get("deleted_obs_count", 0) for s in summaries),
        "modified_obs_count": sum(s.get("modified_obs_count", 0) for s in summaries),
        "current_schema_count": sum(s.get("current_schema_count", 0) for s in summaries),
        "previous_schema_count": sum(s.get("previous_schema_count", 0) for s in summaries),
        "added_schema_count": sum(s.get("added_schema_count", 0) for s in summaries),
        "deleted_schema_count": sum(s.get("deleted_schema_count", 0) for s in summaries),
        "modified_schema_count": sum(s.get("modified_schema_count", 0) for s in summaries),
        "obs_diff_count": sum(s.get("obs_diff_count", 0) for s in summaries),
        "schema_diff_count": sum(s.get("schema_diff_count", 0) for s in summaries),
        "partitions_count": len(summaries),
        "is_multi_input_aggregated": True
    }

def fetch_deleted_nodes_sample(run_path: str, limit: int = 15) -> List[Dict[str, Any]]:
    blobs = _get_run_blobs(run_path)
    all_nodes = []
    
    # 1. Look for any nodes-deleted.mcf across all subpartitions
    mcf_blobs = [b for b in blobs if b.name.endswith("nodes-deleted.mcf")]
    for blob in mcf_blobs:
        try:
            # Download up to 50MB
            content_bytes = blob.download_as_bytes(start=0, end=52428800)
            content_str = content_bytes.decode('utf-8', errors='replace')
            if "Node:" in content_str:
                nodes = _parse_mcf_deleted_nodes(content_str, limit - len(all_nodes))
                all_nodes.extend(nodes)
                if len(all_nodes) >= limit:
                    log.info(f"Loaded {len(all_nodes)} deleted nodes across MCF partitions")
                    return all_nodes[:limit]
        except Exception as e:
            log.warning(f"Error reading MCF blob {blob.name}: {e}")

    if all_nodes:
        return all_nodes[:limit]

    # 2. Look for any obs_diff_log.csv across all subpartitions
    csv_blobs = [b for b in blobs if b.name.endswith("obs_diff_log.csv")]
    for blob in csv_blobs:
        try:
            content_bytes = blob.download_as_bytes(start=0, end=52428800)
            content_str = content_bytes.decode('utf-8', errors='replace')
            if content_str.strip():
                nodes = _parse_csv_deleted_nodes(content_str, limit - len(all_nodes))
                all_nodes.extend(nodes)
                if len(all_nodes) >= limit:
                    log.info(f"Loaded {len(all_nodes)} deleted nodes across CSV diff logs")
                    return all_nodes[:limit]
        except Exception as e:
            log.warning(f"Error reading CSV diff log blob {blob.name}: {e}")
            
    return all_nodes[:limit]

def _parse_csv_deleted_nodes(content: str, limit: int) -> List[Dict[str, Any]]:
    parsed_nodes = []
    lines = content.strip().splitlines()
    if len(lines) > 2:
        lines = lines[:-1]
        
    reader = csv.reader(lines)
    header = next(reader, None)
    if not header:
        return []
    
    try:
        key_idx = header.index("key_combined")
        type_idx = header.index("diff_type")
    except ValueError:
        return []
        
    count = 0
    for row in reader:
        if count >= limit:
            break
        if len(row) > max(key_idx, type_idx) and row[type_idx].strip() == "DELETED":
            key_val = row[key_idx].strip()
            tokens = key_val.split(';')
            if len(tokens) >= 3:
                sv = tokens[0].replace("dcid:", "")
                place = tokens[1].replace("dcid:", "")
                date = tokens[2].strip()
                
                parsed_nodes.append({
                    "variableMeasured": sv,
                    "observationAbout": place,
                    "observationDate": date
                })
                count += 1
                
    return parsed_nodes

def _parse_mcf_deleted_nodes(content: str, limit: int) -> List[Dict[str, Any]]:
    nodes = content.strip().split('\n\n')
    if len(nodes) > 1:
        if "typeOf:" not in nodes[-1]:
            nodes = nodes[:-1]
            
    parsed_nodes = []
    
    for node in nodes[:limit]:
        lines = node.strip().split('\n')
        node_data = {}
        for line in lines:
            parts = line.split(':')
            if len(parts) >= 2:
                key = parts[0].strip()
                val = ':'.join(parts[1:]).strip()
                node_data[key] = val
        if node_data:
            parsed_nodes.append(node_data)
            
    return parsed_nodes

from google.cloud import storage

NL_STATVARS_GCS = "gs://unresolved_mcf/import_validation/nl_statvars.csv"
_CACHED_NL_STATVARS: set[str] | None = None

def get_nl_statvars_set() -> set[str]:
    global _CACHED_NL_STATVARS
    if _CACHED_NL_STATVARS is not None:
        return _CACHED_NL_STATVARS
    try:
        from .golden_generator import load_set_from_gcs
        client = storage.Client()
        _CACHED_NL_STATVARS = load_set_from_gcs(client, NL_STATVARS_GCS)
        log.info(f"Loaded {len(_CACHED_NL_STATVARS)} official NL statvars from {NL_STATVARS_GCS}")
        
        # Supplement with BigQuery NLStatVars table
        try:
            bq = bigquery.Client(project=settings.project_id)
            query = "SELECT DISTINCT id FROM `datcom-store.dc_kg_latest.NLStatVars`"
            for row in bq.query(query).result():
                if row.id:
                    _CACHED_NL_STATVARS.add(row.id)
            log.info(f"Total curated NL statvars in cache (GCS + BigQuery): {len(_CACHED_NL_STATVARS)}")
        except Exception as bq_err:
            log.warning(f"Could not supplement from BigQuery NLStatVars: {bq_err}")
            
        return _CACHED_NL_STATVARS
    except Exception as e:
        log.error(f"Failed to load NL statvars: {e}")
        return set()

def check_nl_search_impact(svs: List[str]) -> List[str]:
    """
    Checks if any of the Statistical Variables are in the official Data Commons NL search validator list (gs://unresolved_mcf/import_validation/nl_statvars.csv).
    """
    if not svs:
        return []
    nl_set = get_nl_statvars_set()
    return [sv for sv in svs if sv in nl_set]

def aggregate_deleted_variables(deleted_nodes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Groups deleted nodes by unique Statistical Variable names and returns a sorted list of count metrics with NL status.
    """
    counts = {}
    for node in deleted_nodes:
        var = node.get("variableMeasured", "Unknown")
        # Clean variable Measured
        if var.startswith("dcid:"):
            var = var.replace("dcid:", "")
        counts[var] = counts.get(var, 0) + 1
        
    sv_list = list(counts.keys())
    nl_impact_svs = set(check_nl_search_impact(sv_list))
    
    aggregated = [
        {
            "variable": var, 
            "count": count,
            "has_nl_impact": var in nl_impact_svs
        } 
        for var, count in counts.items()
    ]
    # Sort by count descending
    aggregated.sort(key=lambda x: x["count"], reverse=True)
    return aggregated


def fetch_provenance_from_manifest(run_path: str) -> Tuple[str | None, str | None]:
    import re
    # Strip any trailing /input0, /input1, etc. to get the root run folder path
    clean_path = re.sub(r'/input\d+/?$', '', run_path.strip())
    manifest_path = clean_path.rstrip('/') + "/manifest.json"
    try:
        res = subprocess.run([
            settings.gcloud_bin_path,
            "storage", "cat", manifest_path
        ], capture_output=True, text=True)
        
        if res.returncode == 0:
            manifest = json.loads(res.stdout.strip())
            specs = manifest.get("import_specifications", [])
            if specs:
                first_spec = specs[0]
                url = first_spec.get("provenance_url")
                desc = first_spec.get("provenance_description")
                return url, desc
    except Exception as e:
        log.warning(f"Failed to read manifest.json from GCS at {manifest_path}: {e}")
        
    return None, None


def check_golden_config_status(run_path: str) -> bool:
    """Checks if validation_config.json or merged_validation_config.json exists in any partition and has a GOLDENS_CHECK rule."""
    blobs = _get_run_blobs(run_path)
    config_blobs = [b for b in blobs if b.name.endswith("validation_config.json") or b.name.endswith("merged_validation_config.json")]
    for blob in config_blobs:
        try:
            content = blob.download_as_text()
            if content.strip():
                config = json.loads(content.strip())
                rules = config.get("rules", [])
                for rule in rules:
                    if rule.get("validator") == "GOLDENS_CHECK":
                        log.info(f"Detected existing golden validation check in blob {blob.name}")
                        return True
        except Exception as e:
            log.debug(f"Failed to check golden config at blob {blob.name}: {e}")
    return False


def fetch_validation_failures(run_path: str) -> Dict[str, Any] | None:
    """
    Parses validation_output.csv blobs across all partitions under run_path.
    Extracts failed validation checks and messages when Cloud Logging is unavailable.
    """
    blobs = _get_run_blobs(run_path)
    val_blobs = [b for b in blobs if b.name.endswith("validation_output.csv")]
    failed_checks = []
    
    for blob in val_blobs:
        try:
            content = blob.download_as_text()
            lines = content.strip().splitlines()
            if not lines:
                continue
            reader = csv.DictReader(lines)
            for row in reader:
                st = (row.get("Status") or "").upper()
                if st in ["FAILED", "DATA_ERROR", "ERROR"]:
                    name = row.get("ValidationName", "")
                    msg = row.get("Message", "")
                    part_match = re.search(r'input\d+', blob.name)
                    part_str = f" ({part_match.group(0)})" if part_match else ""
                    if msg:
                        failed_checks.append(f"{msg}{part_str}")
                    elif name:
                        failed_checks.append(f"Failed check: {name} [{st}]{part_str}")
        except Exception as e:
            log.warning(f"Failed to read validation_output.csv blob {blob.name}: {e}")
            
    if not failed_checks:
        return None
        
    error_msg = " ".join(failed_checks)
    has_missing_refs = any("missing_refs" in c.lower() or "missing reference" in c.lower() for c in failed_checks)
    has_lint = any("lint" in c.lower() for c in failed_checks)
    has_baseline_missing = any("previous_obs_count" in c.lower() or "differ summary is missing" in c.lower() for c in failed_checks)
    
    classification = "SCHEMA_VALIDATION_ERROR"
    recommendation = "Resolve conflicting node properties or validation rules in the import schema."
    if has_baseline_missing and not has_missing_refs and not has_lint:
        recommendation = "Initial run missing prior production baseline on GCS. Add validation_config.json override to bypass baseline deletion checks."
    elif has_missing_refs and not has_lint:
        recommendation = "Resolve missing DCID entities or update node reference definitions."
    elif has_lint:
        recommendation = "Fix syntax/lint formatting errors in the generated MCF files."
        
    return {
        "classification": classification,
        "error_message": error_msg,
        "recommendation": recommendation,
        "raw_logs_snippet": "Validation output failures:\n" + "\n".join(failed_checks)
    }
