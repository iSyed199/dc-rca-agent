import io
import csv
import logging
import re
import pandas as pd
from typing import Set, Tuple, List, Dict, Any
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from ..settings import settings

log = logging.getLogger(__name__)

# Constants for Golden filtering reference files
NL_STATVARS_GCS = "gs://unresolved_mcf/import_validation/nl_statvars.csv"
PLACES_100K_GCS = "gs://unresolved_mcf/import_validation/top_100k_places.csv"

# Column alias dictionary for flexible header mapping
COLUMN_ALIASES = {
    "variableMeasured": [
        "variablemeasured", "variable_measured", "sv_name", "statvar", "stat_var", 
        "variable", "statvarname", "stat_var_name", "var"
    ],
    "observationAbout": [
        "observationabout", "observation_about", "place", "location", "school_state_code", 
        "geoid", "entity", "placedcid", "place_dcid", "area"
    ],
    "observationDate": [
        "observationdate", "observation_date", "date", "year", "time", "obs_date", "obsdate"
    ],
    "unit": ["unit", "units"],
    "scalingFactor": ["scalingfactor", "scaling_factor", "multiplier"],
    "observationPeriod": ["observationperiod", "observation_period", "obs_period", "period"],
    "measurementMethod": ["measurementmethod", "measurement_method", "mmethod", "method"]
}

def parse_gcs_path(path: str) -> Tuple[str, str]:
    """Parse gs://bucket/path/to/file into (bucket, path)"""
    if not path.startswith("gs://"):
        raise ValueError(f"Invalid GCS path: {path}")
    parts = path[5:].split("/", 1)
    bucket = parts[0]
    blob = parts[1] if len(parts) > 1 else ""
    return bucket, blob

def clean_prefix(val: str) -> str:
    """Clean namespace prefixes (dcid:, dcs:) from values."""
    if not val:
        return ""
    val = str(val).strip()
    if val.startswith("dcid:"):
        return val[5:]
    if val.startswith("dcs:"):
        return val[4:]
    return val

def load_set_from_gcs(client: storage.Client, gcs_path: str) -> Set[str]:
    """Download a single-column CSV from GCS and load values into a Python set."""
    bucket_name, blob_name = parse_gcs_path(gcs_path)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    log.info(f"Loading reference filter set from {gcs_path}...")
    try:
        content = blob.download_as_text(encoding="utf-8")
        results: Set[str] = set()
        reader = csv.reader(io.StringIO(content))
        for row in reader:
            if row:
                val = clean_prefix(row[0])
                if val and val != "dcid" and val != "observationAbout":
                    results.add(val)
        log.info(f"Loaded {len(results)} items from {gcs_path}.")
        return results
    except Exception as e:
        log.error(f"Error loading reference set from {gcs_path}: {e}")
        return set()

def _detect_columns(headers: List[str]) -> Dict[str, str]:
    """Dynamically map CSV headers to standard output property names ensuring 1-to-1 mapping."""
    mapping = {}
    cleaned_headers = {h: re.sub(r'[^a-zA-Z0-9_]', '', h.strip().lower()) for h in headers}
    
    assigned_std = set()
    for original_h, clean_h in cleaned_headers.items():
        for std_col, aliases in COLUMN_ALIASES.items():
            if std_col not in assigned_std and clean_h in aliases:
                mapping[original_h] = std_col
                assigned_std.add(std_col)
                break
    return mapping

def _filter_single_csv_blob(
    run_bucket_name: str,
    blob_name: str,
    nl_statvars: Set[str],
    places_100k: Set[str],
    output_headers: List[str],
    max_fallback_samples: int = 500
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], int]:
    """Fast-streams, maps, and filters observations from a GCS CSV file using byte-range slices and Pandas.
    
    Returns:
        (nl_matched_rows, fallback_sample_rows, scanned_count)
    """
    client = storage.Client()
    bucket = client.bucket(run_bucket_name)
    blob = bucket.blob(blob_name)
    
    log.info(f"Filtering blob: {blob.name}...")
    
    matched_rows = []
    fallback_rows = []
    scanned_count = 0
    
    try:
        # Download up to 20MB slice for rapid baseline extraction
        max_bytes = 20_000_000
        data = blob.download_as_bytes(start=0, end=max_bytes)
        
        # Trim to last complete line
        last_nl = data.rfind(b'\n')
        if last_nl > 0:
            data = data[:last_nl]
            
        df = pd.read_csv(io.BytesIO(data), dtype=str, na_filter=False, on_bad_lines='skip')
        scanned_count = len(df)
        headers = list(df.columns)
        col_mapping = _detect_columns(headers)
        
        # We need at minimum a variable column and a place column
        var_orig = None
        place_orig = None
        for orig, std in col_mapping.items():
            if std == "variableMeasured":
                var_orig = orig
            elif std == "observationAbout":
                place_orig = orig
                
        if not var_orig or not place_orig:
            log.info(f"Skipping non-observation CSV {blob.name} (missing variable or place column).")
            return matched_rows, fallback_rows, scanned_count
            
        # Fast vectorized prefix stripping for dcid: and dcs:
        var_clean = df[var_orig].astype(str).str.replace(r'^(dcid:|dcs:)', '', regex=True).str.strip()
        place_clean = df[place_orig].astype(str).str.replace(r'^(dcid:|dcs:)', '', regex=True).str.strip()
        
        # Tier 1: Match against NL StatVars & Top 100k Places
        mask = var_clean.isin(nl_statvars) & place_clean.isin(places_100k)
        matched_chunk = df[mask]
        
        if not matched_chunk.empty:
            renamed = matched_chunk.rename(columns=col_mapping)
            for col in output_headers:
                if col not in renamed.columns:
                    renamed[col] = ""
            final_df = renamed.loc[:, ~renamed.columns.duplicated()][output_headers].copy()
            final_df["variableMeasured"] = final_df["variableMeasured"].astype(str).str.replace(r'^(dcid:|dcs:)', '', regex=True).str.strip()
            final_df["observationAbout"] = final_df["observationAbout"].astype(str).str.replace(r'^(dcid:|dcs:)', '', regex=True).str.strip()
            matched_rows.extend(final_df.to_dict(orient="records"))
            
        # Tier 2: Collect high-quality fallback sample
        sample_df = df.head(max_fallback_samples).copy()
        sample_renamed = sample_df.rename(columns=col_mapping)
        for col in output_headers:
            if col not in sample_renamed.columns:
                sample_renamed[col] = ""
        sample_final = sample_renamed.loc[:, ~sample_renamed.columns.duplicated()][output_headers].copy()
        sample_final["variableMeasured"] = sample_final["variableMeasured"].astype(str).str.replace(r'^(dcid:|dcs:)', '', regex=True).str.strip()
        sample_final["observationAbout"] = sample_final["observationAbout"].astype(str).str.replace(r'^(dcid:|dcs:)', '', regex=True).str.strip()
        fallback_rows.extend(sample_final.to_dict(orient="records"))
        
        log.info(f"Finished blob {blob.name}. Scanned: {scanned_count}, Matched: {len(matched_rows)}, Fallback sample: {len(fallback_rows)}")
    except Exception as e:
        log.exception(f"Error filtering blob {blob_name} via Pandas: {e}")
        
    return matched_rows, fallback_rows, scanned_count

def generate_goldens_in_gcs(issue_id: str, run_folder: str) -> Dict[str, Any]:
    """Generate golden summary and observations files and save them to GCS.
    
    Returns:
        Dict detailing the paths of the generated golden files and counts.
    """
    log.info(f"Starting golden baseline generation for issue_id={issue_id}, run_folder={run_folder}")
    
    # 1. Initialize client
    client = storage.Client()
    
    # 2. Load the reference filter sets
    nl_statvars = load_set_from_gcs(client, NL_STATVARS_GCS)
    places_100k = load_set_from_gcs(client, PLACES_100K_GCS)
    
    # Parse run folder bucket/prefix
    run_bucket_name, run_prefix = parse_gcs_path(run_folder)
    run_prefix = run_prefix.rstrip('/') + '/'
    run_bucket = client.bucket(run_bucket_name)
    
    run_name = run_folder.rstrip('/').split('/')[-1]
    golden_summary_path = f"historical_archives/{issue_id}/{run_name}/golden_data/golden_summary_report.csv"
    golden_obs_path = f"historical_archives/{issue_id}/{run_name}/golden_data/golden_observations.csv"
    
    golden_summary_blob = run_bucket.blob(golden_summary_path)
    golden_obs_blob = run_bucket.blob(golden_obs_path)
    
    # 3. Step 1: Generate Consolidated Golden Summary Report
    # Scan all summary_report.csv blobs under run_prefix (root and sub-inputs)
    all_blobs = list(client.list_blobs(run_bucket_name, prefix=run_prefix))
    summary_blobs = [b for b in all_blobs if b.name.endswith("summary_report.csv")]
    
    summary_copied = False
    target_cols = [
        "StatVar", "NumPlaces", "MinDate", "MeasurementMethods", 
        "Units", "ScalingFactors", "observationPeriods"
    ]
    
    if summary_blobs:
        log.info(f"Found {len(summary_blobs)} summary_report.csv file(s) to consolidate.")
        seen_statvars = set()
        output_io = io.StringIO()
        writer = csv.DictWriter(output_io, fieldnames=target_cols)
        writer.writeheader()
        
        total_summary_rows = 0
        for sb in summary_blobs:
            try:
                content = sb.download_as_text(encoding="utf-8")
                reader = csv.DictReader(io.StringIO(content))
                headers = reader.fieldnames or []
                
                # Map header variations (e.g., stat_var, statvar, StatVar)
                col_map = {}
                for h in headers:
                    clean_h = h.strip().lower()
                    if clean_h in ["statvar", "stat_var", "sv_name", "variable"]:
                        col_map["StatVar"] = h
                    elif clean_h in ["numplaces", "num_places"]:
                        col_map["NumPlaces"] = h
                    elif clean_h in ["mindate", "min_date"]:
                        col_map["MinDate"] = h
                    elif clean_h in ["measurementmethods", "measurement_methods", "methods"]:
                        col_map["MeasurementMethods"] = h
                    elif clean_h in ["units", "unit"]:
                        col_map["Units"] = h
                    elif clean_h in ["scalingfactors", "scaling_factors"]:
                        col_map["ScalingFactors"] = h
                    elif clean_h in ["observationperiods", "observation_periods"]:
                        col_map["observationPeriods"] = h
                        
                for row in reader:
                    sv = clean_prefix(row.get(col_map.get("StatVar", "StatVar"), ""))
                    if not sv or sv in seen_statvars:
                        continue
                    seen_statvars.add(sv)
                    
                    filtered_row = {
                        "StatVar": sv,
                        "NumPlaces": row.get(col_map.get("NumPlaces", "NumPlaces"), ""),
                        "MinDate": row.get(col_map.get("MinDate", "MinDate"), ""),
                        "MeasurementMethods": row.get(col_map.get("MeasurementMethods", "MeasurementMethods"), ""),
                        "Units": row.get(col_map.get("Units", "Units"), ""),
                        "ScalingFactors": row.get(col_map.get("ScalingFactors", "ScalingFactors"), ""),
                        "observationPeriods": row.get(col_map.get("observationPeriods", "observationPeriods"), "")
                    }
                    writer.writerow(filtered_row)
                    total_summary_rows += 1
            except Exception as e:
                log.error(f"Error processing summary blob {sb.name}: {e}")
                
        if total_summary_rows > 0:
            golden_summary_blob.upload_from_string(output_io.getvalue(), content_type="text/csv")
            log.info(f"Uploaded Consolidated Golden Summary Report ({total_summary_rows} rows) to {golden_summary_path}.")
            summary_copied = True
    else:
        log.warning(f"No summary_report.csv found under {run_prefix}.")
        
    # 4. Step 2: Generate Golden Observations
    # Find all observation CSV files directly in run folder root or generated folders
    csv_blobs: List[storage.Blob] = []
    for blob in all_blobs:
        if not blob.name.endswith(".csv"):
            continue
        rel_path = blob.name[len(run_prefix):]
        # Ignore raw source_files, historical_archives, or summary reports
        if "source_files/" in rel_path or "historical_archives/" in rel_path or "summary_report" in rel_path or "golden_data/" in rel_path:
            continue
        csv_blobs.append(blob)
        
    if not csv_blobs:
        log.warning("No output CSV files found in run folder. Skipping observations golden generation.")
        return {
            "success": summary_copied,
            "summary_golden": f"gs://{run_bucket_name}/{golden_summary_path}" if summary_copied else None,
            "observations_golden": None,
            "message": "Summary golden generated. No output observation CSVs found."
        }
        
    # Cap to max 10 representative CSV files if dataset has dozens/hundreds of files (e.g. NOAA 99 files)
    scan_blobs = csv_blobs[:10] if len(csv_blobs) > 10 else csv_blobs
    log.info(f"Found {len(csv_blobs)} output CSV files (scanning {len(scan_blobs)} files for baseline observations): {[b.name.split('/')[-1] for b in scan_blobs]}")
    
    output_headers = [
        "variableMeasured", "unit", "scalingFactor", "observationPeriod", 
        "measurementMethod", "observationAbout", "observationDate"
    ]
    
    all_matched_rows = []
    all_fallback_rows = []
    total_scanned = 0
    
    log.info(f"Filtering {len(scan_blobs)} CSV files concurrently using ThreadPoolExecutor...")
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(
                _filter_single_csv_blob,
                run_bucket_name,
                blob.name,
                nl_statvars,
                places_100k,
                output_headers,
                500  # sample limit per blob for fallback
            )
            for blob in scan_blobs
        ]
        
        for future in futures:
            try:
                matched, fallback, scanned = future.result()
                total_scanned += scanned
                all_matched_rows.extend(matched)
                all_fallback_rows.extend(fallback)
            except Exception as e:
                log.error(f"Error gathering thread filtered results: {e}")
                
    # Select rows to write: Tier 1 (NL Matched) if available, otherwise Tier 2 (Adaptive Fallback Sample)
    output_io = io.StringIO()
    writer = csv.DictWriter(output_io, fieldnames=output_headers)
    writer.writeheader()
    
    if all_matched_rows:
        rows_to_write = all_matched_rows
        log.info(f"Using {len(rows_to_write)} NL & Top-100k matched observations.")
    else:
        # Fallback to representative sample so the golden file is NEVER empty
        rows_to_write = all_fallback_rows[:2000]
        log.info(f"0 NL matches found. Fallback to {len(rows_to_write)} representative sample observations across dataset.")
        
    for row in rows_to_write:
        writer.writerow(row)
        
    # Upload generated golden observations
    golden_obs_blob.upload_from_string(output_io.getvalue(), content_type="text/csv")
    log.info(f"Uploaded Golden Observations ({len(rows_to_write)} rows out of {total_scanned} scanned) to {golden_obs_path}.")
    
    return {
        "success": True,
        "summary_golden": f"gs://{run_bucket_name}/{golden_summary_path}" if summary_copied else None,
        "observations_golden": f"gs://{run_bucket_name}/{golden_obs_path}",
        "scanned_rows": total_scanned,
        "matched_rows": len(rows_to_write),
        "message": f"Successfully generated golden baseline files. Scanned: {total_scanned:,}, Baseline observations: {len(rows_to_write):,}."
    }

