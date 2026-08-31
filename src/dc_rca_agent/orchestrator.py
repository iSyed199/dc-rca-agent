import logging
from datetime import datetime, timezone
from .models import FailureEvent
from .database import db
from .sse import sse_manager
from .stage1_forensic.path_resolver import resolve_gcs_run_folder
from .stage1_forensic.validation_reader import (
    fetch_differ_summary,
    fetch_deleted_nodes_sample,
    fetch_provenance_from_manifest,
    check_golden_config_status,
    fetch_validation_failures
)

log = logging.getLogger(__name__)

async def run_stages(event: FailureEvent, update_watermark: bool = True) -> None:
    # Check if the event is a SUCCESS event (FINISH SUCCESS)
    if event.status == "SUCCESS":
        log.info(f"Processing SUCCESS event for import '{event.import_name}' (job_id={event.job_id}) to check for auto-resolution...")
        # Check matching items first outside the lock as a fast-path
        results = db.get_all_results()
        matching_items = [item for item in results if item.get('title') == event.import_name and item.get('status') != 'RESOLVED']
        
        if matching_items:
            import asyncio
            run_folder = await asyncio.to_thread(resolve_gcs_run_folder, event)
            diff_summary = await asyncio.to_thread(fetch_differ_summary, run_folder) if run_folder else None
            provenance_url, provenance_desc = await asyncio.to_thread(fetch_provenance_from_manifest, run_folder) if run_folder else (None, None)
            has_golden_config = await asyncio.to_thread(check_golden_config_status, run_folder) if run_folder else False
            
            # Acquire lock to perform atomic read-modify-write
            async with db.write_lock:
                # Re-fetch results inside the lock to get the absolute latest state
                current_results = db.get_all_results()
                active_matching_items = [item for item in current_results if item.get('title') == event.import_name and item.get('status') != 'RESOLVED']
                
                resolved_any = False
                for item in active_matching_items:
                    # Only resolve if the successful run is newer than the active failure
                    failure_time_str = item.get('run_time')
                    if failure_time_str and event.timestamp:
                        try:
                            failure_ts = datetime.fromisoformat(failure_time_str.replace('Z', '+00:00'))
                            if event.timestamp <= failure_ts:
                                log.info(f"Skipping auto-resolution for '{event.import_name}': SUCCESS event ({event.timestamp}) is not newer than active failure ({failure_ts}).")
                                continue
                        except Exception as te:
                            log.warning(f"Error parsing failure run_time '{failure_time_str}' for comparison: {te}")

                    target_job_id = item.get('job_id')
                    log.info(f"Auto-resolving active failure for '{event.import_name}' (job_id={target_job_id}). Transitioning status to RESOLVED.")
                    db.resolve_failure_record(
                        target_job_id=target_job_id,
                        run_folder=run_folder,
                        diff_summary=diff_summary,
                        provenance_url=provenance_url,
                        provenance_description=provenance_desc,
                        successful_job_id=event.job_id,
                        successful_job_name=event.job_name,
                        successful_run_time=event.timestamp.isoformat() if event.timestamp else datetime.now(timezone.utc).isoformat(),
                        successful_region=event.region,
                        successful_project_id=event.project_id,
                        has_golden_config=has_golden_config
                    )
                    resolved_any = True
                
                if resolved_any:
                    now_str = datetime.now(timezone.utc).isoformat()
                    if update_watermark:
                        try:
                            db.update_last_sync_time(now_str)
                        except Exception as e:
                            log.error(f"Failed to update last sync time: {e}")
                        
                    await sse_manager.broadcast({
                        "type": "stage_update",
                        "import_name": event.import_name,
                        "status": "RESOLVED",
                        "latest_run_folder": run_folder,
                        "differ_summary": diff_summary,
                        "provenance_url": provenance_url,
                        "provenance_description": provenance_desc,
                        "has_golden_config": has_golden_config,
                        "job_id": event.job_id,
                        "last_sync_time": now_str if update_watermark else None
                    })
                    log.info(f"Successfully auto-resolved '{event.import_name}'!")
        else:
            log.info(f"SUCCESS event for '{event.import_name}' skipped (no active failures found in database).")
        return

    log.info(f"Starting RCA triage pipeline for import '{event.import_name}' (job_id={event.job_id})")
    
    await sse_manager.broadcast({
        "type": "stage_start",
        "import_name": event.import_name,
        "job_id": event.job_id
    })
    
    # Stage 1: Path Resolution
    import asyncio
    run_folder = await asyncio.to_thread(resolve_gcs_run_folder, event)
    if not run_folder:
        log.error("Stage 1 Failed: Could not resolve GCS run folder.")
        # Automatically run log triage for ingestion failures
        if not event.log_diagnosis and event.job_id:
            try:
                from .main import diagnose_job_logs_async
                event.log_diagnosis = await diagnose_job_logs_async(event.job_id)
            except Exception as e:
                log.warning(f"Auto log diagnosis failed for {event.job_id}: {e}")

        async with db.write_lock:
            db.save_or_update_result(event, "NO_RUN_FOLDERS_FOUND", None, None, log_diagnosis=event.log_diagnosis)
            now_str = datetime.now(timezone.utc).isoformat()
            if update_watermark:
                try:
                    db.update_last_sync_time(now_str)
                except Exception as e:
                    log.error(f"Failed to update last sync time: {e}")

        await sse_manager.broadcast({
            "type": "stage_update",
            "import_name": event.import_name,
            "status": "NO_RUN_FOLDERS_FOUND",
            "latest_run_folder": None,
            "differ_summary": None,
            "job_id": event.job_id,
            "log_diagnosis": event.log_diagnosis,
            "last_sync_time": now_str if update_watermark else None
        })
        return
        
    log.info(f"Stage 1 Success: Resolved run folder to {run_folder}")
    
    # Stage 2: Read Validation Metrics
    diff_summary = await asyncio.to_thread(fetch_differ_summary, run_folder)
    provenance_url, provenance_desc = await asyncio.to_thread(fetch_provenance_from_manifest, run_folder)
    has_golden_config = await asyncio.to_thread(check_golden_config_status, run_folder)
    
    # Fallback to GCS validation_output.csv if Cloud Logging was unavailable or unknown
    if not event.log_diagnosis or event.log_diagnosis.get("classification") in ["UNKNOWN", "PENDING_INDEXING"]:
        val_diagnosis = await asyncio.to_thread(fetch_validation_failures, run_folder)
        if val_diagnosis:
            event.log_diagnosis = val_diagnosis
            log.info(f"Extracted log diagnosis from GCS validation_output.csv: {val_diagnosis['error_message']}")

    if diff_summary:
        log.info(f"Differ Summary details: {diff_summary}")
        async with db.write_lock:
            db.save_or_update_result(
                event, "SUCCESS", run_folder, diff_summary,
                provenance_url=provenance_url,
                provenance_description=provenance_desc,
                has_golden_config=has_golden_config,
                log_diagnosis=event.log_diagnosis
            )
            now_str = datetime.now(timezone.utc).isoformat()
            if update_watermark:
                try:
                    db.update_last_sync_time(now_str)
                except Exception as e:
                    log.error(f"Failed to update last sync time: {e}")
            
        await sse_manager.broadcast({
            "type": "stage_update",
            "import_name": event.import_name,
            "status": "SUCCESS",
            "latest_run_folder": run_folder,
            "differ_summary": diff_summary,
            "provenance_url": provenance_url,
            "provenance_description": provenance_desc,
            "has_golden_config": has_golden_config,
            "job_id": event.job_id,
            "log_diagnosis": event.log_diagnosis,
            "last_sync_time": now_str if update_watermark else None
        })
        
        deleted_count = diff_summary.get('deleted_obs_count', 0)
        added_count = diff_summary.get('added_obs_count', 0)
        log.info(f"Validation metrics - Deleted Observations: {deleted_count}, Added: {added_count}")
        
        if deleted_count > 0:
            deleted_nodes = await asyncio.to_thread(fetch_deleted_nodes_sample, run_folder, limit=5)
            log.info(f"Sample deleted nodes: {deleted_nodes}")
    else:
        log.warning("No differ summary metrics found in run folder validation outputs.")
        # Automatically run log triage for validation differ missing failures
        if not event.log_diagnosis and event.job_id:
            try:
                from .main import diagnose_job_logs_async
                event.log_diagnosis = await diagnose_job_logs_async(event.job_id)
            except Exception as e:
                log.warning(f"Auto log diagnosis failed for {event.job_id}: {e}")

        async with db.write_lock:
            db.save_or_update_result(
                event, "NO_DIFFER_SUMMARY_FOUND", run_folder, None,
                provenance_url=provenance_url,
                provenance_description=provenance_desc,
                has_golden_config=has_golden_config,
                log_diagnosis=event.log_diagnosis
            )
            now_str = datetime.now(timezone.utc).isoformat()
            if update_watermark:
                try:
                    db.update_last_sync_time(now_str)
                except Exception as e:
                    log.error(f"Failed to update last sync time: {e}")
            
        await sse_manager.broadcast({
            "type": "stage_update",
            "import_name": event.import_name,
            "status": "NO_DIFFER_SUMMARY_FOUND",
            "latest_run_folder": run_folder,
            "differ_summary": None,
            "provenance_url": provenance_url,
            "provenance_description": provenance_desc,
            "has_golden_config": has_golden_config,
            "job_id": event.job_id,
            "log_diagnosis": event.log_diagnosis,
            "last_sync_time": now_str if update_watermark else None
        })

