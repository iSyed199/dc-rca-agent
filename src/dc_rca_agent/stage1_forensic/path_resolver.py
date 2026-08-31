from __future__ import annotations
import logging
import yaml
import re
import subprocess
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from ..settings import settings
from ..models import FailureEvent

# Try importing storage SDK dynamically
try:
    from google.cloud import storage
except ImportError:
    storage = None

log = logging.getLogger(__name__)

# Cache to avoid listing GCS directories repeatedly during sync
_discovered_prefixes_cache = {}

def discover_gcs_prefix(import_name: str) -> str | None:
    if not storage:
        log.warning("Google Cloud Storage library is not installed. Cannot perform dynamic GCS prefix lookup.")
        return None
        
    log.info(f"Dynamically searching GCS for prefix of import '{import_name}'...")
    try:
        client = storage.Client(project=settings.project_id)
        bucket = client.bucket(settings.imports_bucket)
        
        search_roots = ["scripts/", "statvar_imports/"]
        queue = list(search_roots)
        
        while queue:
            current_prefix = queue.pop(0)
            # Fetch common prefixes at this level
            iterator = bucket.list_blobs(prefix=current_prefix, delimiter="/")
            list(iterator) # Consume iterator
            
            for folder in iterator.prefixes:
                folder_basename = folder.rstrip("/").split("/")[-1]
                
                # Rule 1: Skip timestamp folders, run helper folders, and standard internal names
                if (
                    re.match(r'^\d{4}_\d{2}_\d{2}', folder_basename) or
                    re.match(r'^input\d+$', folder_basename.lower()) or
                    folder_basename.lower() in {"source_files", "validation", "provenance", "raw_data", "genmcf", "counters", "golden_data"}
                ):
                    continue

                if folder_basename.lower() == import_name.lower():
                    discovered_prefix = folder.rstrip("/")
                    # Look one level deeper inside this folder for double-nesting (e.g. statistics_poland/statistics_poland)
                    try:
                        sub_iterator = bucket.list_blobs(prefix=folder, delimiter="/")
                        list(sub_iterator)
                        for sub_folder in sub_iterator.prefixes:
                            sub_folder_basename = sub_folder.rstrip("/").split("/")[-1]
                            if sub_folder_basename.lower() == import_name.lower():
                                discovered_prefix = sub_folder.rstrip("/")
                                log.info(f"Discovered nested GCS prefix for '{import_name}': {discovered_prefix}")
                                break
                    except Exception as sub_err:
                        log.warning(f"Error checking subfolders for double-nesting: {sub_err}")
                        
                    log.info(f"Dynamically discovered GCS prefix for '{import_name}': {discovered_prefix}")
                    return discovered_prefix
                
                depth = len(folder.rstrip("/").split("/"))
                if depth <= 5: # Limit depth search
                    queue.append(folder)
    except Exception as e:
        log.error(f"Error dynamically searching GCS for prefix '{import_name}': {e}")
        
    return None

def get_import_path_prefix(import_name: str) -> str:
    # 1. Try to read from local imports.yaml config first
    try:
        with open(settings.imports_config_path, "r") as f:
            config = yaml.safe_load(f) or {}
        imports_map = config.get("imports", {})
        if import_name in imports_map:
            return imports_map[import_name]["path_prefix"]
    except Exception as e:
        log.warning(f"Failed to read get_import_path_prefix from config file: {e}")

    # 2. Try the in-memory discovery cache
    if import_name in _discovered_prefixes_cache:
        cached_val = _discovered_prefixes_cache[import_name]
        if cached_val == "":
            raise ValueError(f"Import '{import_name}' is not configured in imports.yaml and could not be discovered dynamically on GCS (cached negative).")
        return cached_val

    # 3. Perform dynamic GCS lookup
    discovered = discover_gcs_prefix(import_name)
    if discovered:
        _discovered_prefixes_cache[import_name] = discovered
        return discovered

    # Cache negative result as empty string to prevent re-querying GCS
    _discovered_prefixes_cache[import_name] = ""
    raise ValueError(f"Import '{import_name}' is not configured in imports.yaml and could not be discovered dynamically on GCS.")

def resolve_gcs_run_folder(event: FailureEvent) -> str | None:
    if getattr(event, "gcs_path", None):
        log.info(f"Using pre-resolved GCS path from event: {event.gcs_path}")
        return event.gcs_path
        
    # Standard Architectural Rule: If the import failed during the SCRIPT stage,
    # the Python/Bash download script crashed before generating any GCS output folders.
    # We must not attempt chronological fallback to avoid attaching older runs.
    if getattr(event, "stage_name", "") == "SCRIPT" and getattr(event, "status", "") in ["FAILURE", "FAILED"]:
        log.info(f"Import '{event.import_name}' failed in SCRIPT stage; no GCS run folder was created for this execution.")
        return None

    try:
        prefix = get_import_path_prefix(event.import_name)
    except ValueError as e:
        log.error(str(e))
        return None


    # Step 1: Query the INIT log entry to get the exact start time using gcloud CLI
    log.info(f"Querying INIT log for job_id: {event.job_id} in project: {settings.project_id}")
    logger_filter = (
        f'resource.type="batch.googleapis.com/Job" AND '
        f'resource.labels.job_id="{event.job_id}" AND '
        f'jsonPayload.stage_name="INIT"'
    )
    
    try:
        res = subprocess.run([
            settings.gcloud_bin_path,
            "logging", "read", logger_filter,
            "--limit=1", "--format=json",
            f"--project={settings.project_id}"
        ], capture_output=True, text=True, check=True)
        
        entries = json.loads(res.stdout.strip())
        if not entries:
            log.warning(f"Could not find INIT stage log for job_id: {event.job_id}. Falling back to chronological latest run folder.")
            latest_run = _get_latest_chronological_run(prefix, event.timestamp)
            return latest_run
            
        init_entry = entries[0]
        timestamp_str = init_entry.get("timestamp")
        # Normalize fractional seconds to microseconds (6 digits) instead of nanoseconds (9 digits)
        timestamp_str = re.sub(r'(\.\d{6})\d*Z$', r'\1Z', timestamp_str)
        # Parse UTC time
        init_timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=ZoneInfo("UTC"))
    except Exception as e:
        log.error(f"Error querying Cloud Logging via gcloud: {e}")
        latest_run = _get_latest_chronological_run(prefix, event.timestamp)
        return latest_run
    log.info(f"Found INIT log with UTC timestamp: {init_timestamp}")

    # Step 2: Convert to Pacific Time
    la_tz = ZoneInfo("America/Los_Angeles")
    local_timestamp = init_timestamp.astimezone(la_tz)
    log.info(f"Converted Pacific Time timestamp: {local_timestamp}")

    # Build the folder name prefix based on timestamp
    # e.g., 2026_06_30T04_03_40
    folder_prefix = local_timestamp.strftime("%Y_%m_%dT%H_%M_%S")
    full_gcs_prefix = f"gs://{settings.imports_bucket}/{prefix}/{folder_prefix}"
    log.info(f"Searching GCS with prefix: {full_gcs_prefix}")

    # Step 3: Run gcloud storage ls to list matching folders
    try:
        res = subprocess.run([
            settings.gcloud_bin_path,
            "storage", "ls", full_gcs_prefix + "*"
        ], capture_output=True, text=True, check=True)
        
        lines = res.stdout.strip().split('\n')
        folders = [l.strip() for l in lines if l.strip().endswith('/')]
        if folders:
            log.info(f"Resolved run folder: {folders[0]}")
            return folders[0]
    except Exception as e:
        log.error(f"Error listing GCS run folders: {e}")
        
    return None

def _get_latest_chronological_run(prefix: str, event_timestamp: datetime) -> str | None:
    parent_path = f"gs://{settings.imports_bucket}/{prefix}/"
    try:
        res = subprocess.run([
            settings.gcloud_bin_path,
            "storage", "ls", parent_path
        ], capture_output=True, text=True, check=True)
        
        lines = res.stdout.strip().split('\n')
        folders = []
        for line in lines:
            path = line.strip()
            if path.endswith('/'):
                folder_name = [x for x in path.split('/') if x][-1]
                match = re.match(r'^(\d{4})_(\d{2})_(\d{2})T(\d{2})_(\d{2})_(\d{2})', folder_name)
                if match:
                    folders.append((folder_name, path))
                    
        if folders:
            folders.sort(key=lambda x: x[0])
            latest_folder_name, latest_path = folders[-1]
            
            # Check if the folder timestamp is too stale compared to the event timestamp
            match = re.match(r'^(\d{4})_(\d{2})_(\d{2})T(\d{2})_(\d{2})_(\d{2})', latest_folder_name)
            if match:
                dt_str = f"{match.group(1)}-{match.group(2)}-{match.group(3)}T{match.group(4)}:{match.group(5)}:{match.group(6)}"
                la_tz = ZoneInfo("America/Los_Angeles")
                utc_tz = ZoneInfo("UTC")
                
                folder_dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=la_tz)
                folder_dt_utc = folder_dt.astimezone(utc_tz)
                
                # Normalize event_timestamp to UTC
                event_dt_utc = event_timestamp.astimezone(utc_tz) if event_timestamp.tzinfo else event_timestamp.replace(tzinfo=utc_tz)
                
                diff_seconds = abs((event_dt_utc - folder_dt_utc).total_seconds())
                if diff_seconds > 172800:  # 48 hours ceiling
                    log.warning(
                        f"Rejected chronological latest folder '{latest_path}' because it is too stale "
                        f"({diff_seconds/3600:.1f} hours diff from event time {event_dt_utc.isoformat()})."
                    )
                    return None
            
            log.info(f"Chronological fallback resolved within safety limits: {latest_path}")
            return latest_path


    except Exception as e:
        log.error(f"Error running chronological fallback: {e}")
    return None
