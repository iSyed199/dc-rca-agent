from __future__ import annotations
import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from fastapi import BackgroundTasks, FastAPI, HTTPException, status, Response
from fastapi.responses import HTMLResponse, StreamingResponse, FileResponse
import json
import os
from .sse import sse_manager, broadcast_sync
from pydantic import BaseModel
from .database import db
from .models import PubsubPushRequest, LogEntry
from google.cloud import pubsub_v1
from .orchestrator import run_stages
from .settings import settings
from .stage1_forensic.log_parser import parse_pubsub_push, to_failure_event, NotAFailureEvent
from .stage1_forensic.validation_reader import fetch_deleted_nodes_sample, aggregate_deleted_variables
from .stage1_forensic.verifiers import get_verifier
from .stage1_forensic.historical_archives import generate_historical_archive, HISTORICAL_ARCHIVES_DIR
from .stage1_forensic.remediation_verifier import verify_remediation_union
from .stage1_forensic.csv_diff_analyzer import compute_csv_regression_diff

import gc
import ctypes
import subprocess

def cleanup_memory():
    gc.collect()
    try:
        libc = ctypes.CDLL("libc.so.6")
        libc.malloc_trim(0)
    except Exception:
        pass

class UpdateIssueIdRequest(BaseModel):
    issue_id: str

class UpdateRcaDetailRequest(BaseModel):
    rca_detail: str

class OverrideStatusRequest(BaseModel):
    status: str

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger(__name__)

pubsub_streaming_future = None
running_reconciler_task = None

async def reconcile_running_jobs_loop():
    """Periodic background state reconciliation loop for active RUNNING jobs in GCP Batch."""
    log.info("Starting background state reconciliation loop for active RUNNING jobs...")
    while True:
        try:
            await asyncio.sleep(45)  # Reconcile active jobs every 45 seconds
            from .database import db
            all_results = db.get_all_results()
            running_jobs = [r for r in all_results if r.get('status') == 'RUNNING']
            
            if not running_jobs:
                continue
                
            log.info(f"Reconciler: Checking status for {len(running_jobs)} active RUNNING jobs in GCP Batch...")
            needs_sync = False
            for r in running_jobs:
                job_id = r.get('job_id')
                import_name = r.get('title')
                job_name = r.get('job_name') or job_id
                region = r.get('region') or 'us-central1'
                
                # Check Batch job state across GCP regions
                state = None
                for test_loc in [region, 'us-west4', 'us-east4', 'us-central1', 'us-east1']:
                    try:
                        cmd = ['gcloud', 'batch', 'jobs', 'describe', job_name, f'--location={test_loc}', f'--project={settings.project_id}', '--format=json']
                        res = await asyncio.to_thread(subprocess.check_output, cmd, stderr=subprocess.DEVNULL)
                        data = json.loads(res.decode('utf-8'))
                        state = data.get('status', {}).get('state')
                        if state:
                            break
                    except Exception:
                        continue
                        
                if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    log.info(f"Reconciler: Detected completed state '{state}' for running job {job_name} ({import_name}).")
                    needs_sync = True
                    break
                    
            if needs_sync:
                log.info("Reconciler: Triggering automated sync & triage for completed batch jobs...")
                await run_sync_worker()
        except asyncio.CancelledError:
            log.info("Reconciler loop cancelled.")
            break
        except Exception as e:
            log.error(f"Reconciler loop encountered error: {e}", exc_info=True)
            await asyncio.sleep(30)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global pubsub_streaming_future, running_reconciler_task
    
    # Initialize Pub/Sub Pull Subscriber
    try:
        main_loop = asyncio.get_running_loop()
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(settings.project_id, settings.pubsub_subscription_id)
        
        def callback(message):
            try:
                log.info(f"Pub/Sub Pull: Received message ID {message.message_id}")
                log_entry_dict = json.loads(message.data.decode('utf-8'))
                entry = LogEntry.model_validate(log_entry_dict)
                event = to_failure_event(entry)
                
                log.info(f"Pub/Sub Pull: Scheduling triage stages for job {event.job_id} ({event.import_name})")
                asyncio.run_coroutine_threadsafe(run_stages(event, update_watermark=False), main_loop)
                message.ack()
            except NotAFailureEvent as e:
                log.info(f"Pub/Sub Pull: Skipping non-trigger event: {e}")
                message.ack()
            except Exception as e:
                log.error(f"Pub/Sub Pull: Error processing message: {e}")
                message.nack()
                
        log.info(f"Pub/Sub Pull: Starting streaming pull subscriber on {subscription_path}...")
        pubsub_streaming_future = subscriber.subscribe(subscription_path, callback=callback)
    except Exception as e:
        log.error(f"Pub/Sub Pull: Failed to initialize streaming pull subscriber: {e}")

    # Start background State Reconciler loop
    try:
        running_reconciler_task = asyncio.create_task(reconcile_running_jobs_loop())
        log.info("State Reconciler loop scheduled successfully.")
    except Exception as e:
        log.error(f"Failed to start State Reconciler loop: {e}")

    yield

    # Shutdown logic
    if pubsub_streaming_future:
        log.info("Pub/Sub Pull: Stopping streaming pull subscriber...")
        pubsub_streaming_future.cancel()
        log.info("Pub/Sub Pull: Subscriber stopped.")
    if running_reconciler_task:
        log.info("Stopping State Reconciler loop...")
        running_reconciler_task.cancel()
        log.info("State Reconciler loop stopped.")

app = FastAPI(title="Data Commons Import RCA", lifespan=lifespan)

# Paths for static data
INDEX_HTML_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates", "index.html")

@app.get("/", response_class=HTMLResponse)
def get_dashboard():
    try:
        with open(INDEX_HTML_PATH, "r") as f:
            return f.read()

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Template load error: {e}"
        )

FAVICON_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates", "favicon.jpg")

@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    if os.path.exists(FAVICON_PATH):
        return FileResponse(FAVICON_PATH, media_type="image/jpeg")
    return Response(status_code=status.HTTP_204_NO_CONTENT)

@app.get("/api/verification-results")
def get_verification_results():
    try:
        return db.get_all_results()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database load error: {e}"
        )

@app.put("/api/verification-results/{job_id}")
def update_issue_id(job_id: str, payload: UpdateIssueIdRequest):
    try:
        db.update_issue_id(job_id, payload.issue_id)
        return {"status": "updated"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update issue ID: {e}"
        )

@app.put("/api/verification-results/{job_id}/rca-detail")
def update_rca_detail(job_id: str, payload: UpdateRcaDetailRequest):
    try:
        db.update_rca_detail(job_id, payload.rca_detail)
        return {"status": "updated"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update RCA details: {e}"
        )

@app.get("/api/settings")
def get_settings():
    return {
        "project_id": settings.project_id,
        "imports_bucket": settings.imports_bucket
    }

@app.get("/api/stream-updates")
async def stream_updates():
    async def event_generator():
        queue = sse_manager.subscribe()
        try:
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=5.0)
                    yield f"data: {json.dumps(event)}\n\n"
                except asyncio.TimeoutError:
                    # Keep-alive comment to prevent GFE/Cloud Run from closing the idle connection
                    yield ": ping\n\n"
        except asyncio.CancelledError:
            sse_manager.unsubscribe(queue)

    return StreamingResponse(event_generator(), media_type="text/event-stream")

@app.get("/api/deletions-sample/{issue_id}")
def get_deletions_sample(issue_id: str):
    try:
        results = db.get_all_results()
        import_data = next((r for r in results if r.get('issue_id') == issue_id or r.get('job_id') == issue_id or str(r.get('issue_num')) == str(issue_id) or r.get('title') == issue_id), None)
        if not import_data:
            raise HTTPException(status_code=404, detail="Issue not found")

            
        latest_folder = import_data.get('latest_run_folder')
        if not latest_folder:
            return {"sample_mcf": "", "aggregated_svs": []}
            
        log.info(f"Fetching deletions data from GCS for issue {issue_id}...")
        # Fetch a larger sample to ensure correct aggregate counts
        all_nodes = fetch_deleted_nodes_sample(latest_folder, limit=500000)
        if not all_nodes:
            return {"sample_mcf": "", "aggregated_svs": []}
            
        aggregated_svs = aggregate_deleted_variables(all_nodes)
        
        # Limit the rendered text preview to 50 nodes
        sample_nodes = all_nodes[:50]
        mcf_blocks = []
        for idx, node in enumerate(sample_nodes):
            block_lines = [f"Node: deleted-node-{idx}"]
            for k, v in node.items():
                if k != "Node":
                    block_lines.append(f"{k}: {v}")
            mcf_blocks.append('\n'.join(block_lines))
            
        mcf_str = '\n\n'.join(mcf_blocks)
        return {
            "sample_mcf": mcf_str,
            "aggregated_svs": aggregated_svs
        }
    except Exception as e:
        log.error(f"Error resolving deletions sample for issue {issue_id}: {e}")
        return {"sample_mcf": "", "aggregated_svs": []}

@app.get("/api/check-link")
def check_link(url: str):
    import urllib.request
    import urllib.error
    import socket
    
    req = urllib.request.Request(
        url, 
        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            code = response.getcode()
            return {"status": "online", "code": code}
    except urllib.error.HTTPError as e:
        if e.code == 403:
            return {"status": "blocked", "code": 403}
        return {"status": "offline", "code": e.code}
    except urllib.error.URLError as e:
        return {"status": "offline", "code": "connection_failed"}
    except socket.timeout:
        return {"status": "offline", "code": "timeout"}
    except Exception as e:
        return {"status": "offline", "code": "error"}

def run_verification_task(issue_id: str, title: str, latest_folder: str):
    try:
        # Resolve user-friendly provider name
        provider = "upstream data source"
        norm = title.lower()
        if "worldbank" in norm or "world_bank" in norm or "worlddevelopmentindicators" in norm:
            provider = "World Bank API portal"
        elif "census" in norm:
            provider = "US Census Bureau API portal"
        elif "eurostat" in norm:
            provider = "Eurostat API portal"
        elif "oecd" in norm:
            provider = "OECD API portal"
        elif "bls" in norm:
            provider = "Bureau of Labor Statistics API portal"
        elif "who" in norm:
            provider = "World Health Organization API portal"
        elif "un" in norm:
            provider = "UN Data portal"
        elif "fred" in norm:
            provider = "FRED API portal"
            
        # 1. Broadcast START event
        broadcast_sync({
            "type": "verification_progress",
            "issue_id": issue_id,
            "current": 0,
            "total": 5,
            "status": "STARTING",
            "message": f"Connecting to {provider}..."
        })
        
        # 2. Get deleted nodes
        deleted_nodes = fetch_deleted_nodes_sample(latest_folder, limit=5)
        if not deleted_nodes:
            broadcast_sync({
                "type": "verification_progress",
                "issue_id": issue_id,
                "current": 0,
                "total": 0,
                "status": "COMPLETED",
                "overall_status": "NO_DELETIONS",
                "message": "No deletions found."
            })
            return
            
        # 3. Create callback that streams live updates
        def progress_callback(current, total, var, status):
            broadcast_sync({
                "type": "verification_progress",
                "issue_id": issue_id,
                "current": current,
                "total": total,
                "status": "RUNNING",
                "message": f"Auditing variables: {current} of {total} checked..."
            })
            
        # 4. Resolve verifier and verify deletions
        import inspect
        verifier = get_verifier(title)
        sig = inspect.signature(verifier.verify_deletions)
        if "progress_callback" in sig.parameters:
            verification_details = verifier.verify_deletions(deleted_nodes, progress_callback=progress_callback)
        else:
            verification_details = verifier.verify_deletions(deleted_nodes)
        
        # 5. Determine overall status
        statuses = [res["status"] for res in verification_details]
        any_mismatch = any(s == "EXISTS_UPSTREAM" for s in statuses)
        any_needs_key = any(s == "NEEDS_API_KEY" for s in statuses)
        
        if any_mismatch:
            overall_status = "AGENT_MISMATCH_FOUND"
        elif any_needs_key:
            overall_status = "NEEDS_API_KEY"
        else:
            confirmed_count = sum(1 for s in statuses if s in ("VERIFIED_DELETED", "CONFIRMED_DELETED"))
            total_checks = len(statuses)
            if total_checks > 0 and (confirmed_count / total_checks) >= 0.6:
                overall_status = "AGENT_VERIFIED_DELETED"
            else:
                overall_status = "MANUAL_REQUIRED"
                
        # 6. Save results to database
        verification_payload = {
            "overall_status": overall_status,
            "results": verification_details,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        try:
            db.update_verification_results(issue_id, verification_payload)
        except Exception as db_err:
            log.error(f"Failed to save verification results to db: {db_err}")

        # 7. Broadcast COMPLETED event with final stats
        broadcast_sync({
            "type": "verification_progress",
            "issue_id": issue_id,
            "current": len(statuses),
            "total": len(statuses),
            "status": "COMPLETED",
            "overall_status": overall_status,
            "results": verification_details,
            "message": f"Verification completed: {overall_status}"
        })
    except Exception as e:
        log.error(f"Async verification task failed for issue {issue_id}: {e}")
        broadcast_sync({
            "type": "verification_progress",
            "issue_id": issue_id,
            "status": "FAILED",
            "message": f"Verification failed: {str(e)}"
        })

@app.get("/api/verify-upstream/{issue_id}")
def verify_upstream(issue_id: str, background_tasks: BackgroundTasks):
    try:
        results = db.get_all_results()
        import_data = next((r for r in results if r.get('issue_id') == issue_id or r.get('job_id') == issue_id), None)
        if not import_data:
            raise HTTPException(status_code=404, detail="Issue not found")
            
        latest_folder = import_data.get('latest_run_folder')
        if not latest_folder:
            return {"status": "no_run_folder", "results": []}
            
        title = import_data.get('title', '')
        
        # Launch the verification task asynchronously in background
        background_tasks.add_task(run_verification_task, issue_id, title, latest_folder)
        
        return {"status": "PENDING"}
    except Exception as e:
        log.error(f"Error triggering upstream verification for issue {issue_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/verify-upstream/override/{issue_id}")
def verify_upstream_override(issue_id: str, req: OverrideStatusRequest):
    try:
        results = db.get_all_results()
        import_data = next((r for r in results if r.get('job_id') == issue_id or r.get('issue_id') == issue_id), None)
        if not import_data:
            raise HTTPException(status_code=404, detail="Issue not found")
            
        v_res = import_data.get("verification_results")
        if not v_res:
            v_res = {
                "overall_status": req.status,
                "results": [],
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        else:
            v_res["overall_status"] = req.status
            v_res["timestamp"] = datetime.now(timezone.utc).isoformat()
            
            if req.status == "VERIFIED_DELETED":
                for item in v_res.get("results", []):
                    if item.get("status") in ("MANUAL_CHECK_REQUIRED", "UNSUPPORTED_MAPPING"):
                        item["status"] = "CONFIRMED_DELETED"
            
        db.update_verification_results(import_data.get("job_id"), v_res)

        
        broadcast_sync({
            "type": "verification_progress",
            "issue_id": import_data.get("job_id"),
            "current": len(v_res.get("results", [])),
            "total": len(v_res.get("results", [])),
            "status": "COMPLETED",
            "overall_status": req.status,
            "results": v_res.get("results", []),
            "message": f"Verification overridden by user: {req.status}"
        })
        
        return {"status": "success", "verification_results": v_res}
    except Exception as e:
        log.error(f"Error overriding verification status for issue {issue_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/verify-upstream/reset/{issue_id}")
def verify_upstream_reset(issue_id: str):
    try:
        results = db.get_all_results()
        import_data = next((r for r in results if r.get('job_id') == issue_id or r.get('issue_id') == issue_id), None)
        if not import_data:
            raise HTTPException(status_code=404, detail="Issue not found")
            
        target_job_id = import_data.get("job_id", issue_id)
        db.update_verification_results(target_job_id, None)
        
        broadcast_sync({
            "type": "verification_progress",
            "issue_id": target_job_id,
            "status": "RESET",
            "overall_status": None,
            "results": [],
            "message": "Verification results reset."
        })
        
        return {"status": "cleared", "job_id": target_job_id}
    except Exception as e:
        log.error(f"Error resetting verification for issue {issue_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/admin/clear-all-verifications")
def clear_all_verifications():
    try:
        all_results = db.get_all_results()
        cleared_count = 0
        for r in all_results:
            if r.get('verification_results') is not None:
                db.update_verification_results(r.get('job_id'), None)
                cleared_count += 1
        log.info(f"Admin cleared verification_results for {cleared_count} tickets.")
        return {"status": "success", "cleared_count": cleared_count}
    except Exception as e:
        log.error(f"Error clearing all verifications: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/historical-archives/package/{issue_id}")
def generate_archives_package(issue_id: str):
    try:
        package = generate_historical_archive(issue_id)
        if package["count"] == 0:
            return {
                "status": "empty",
                "message": "No historical deletion rows identified for archiving.",
                "commands": []
            }
            
        gcs_dest = f"gs://{settings.imports_bucket}/historical_archives/{issue_id}/{package['run_name']}/historical_archive.csv"
        
        commands = [
            f"gcloud storage cp {package['csv_path']} {gcs_dest}",
            f"git checkout -b archive-{issue_id}",
            f"git add {package['tmcf_path']}",
            f"git commit -m 'Recover historically deleted variables for issue {issue_id}'"
        ]
        
        # Persist generated archive details in Firestore
        results = db.get_all_results()
        import_data = next((r for r in results if r.get('job_id') == issue_id or r.get('issue_id') == issue_id or str(r.get('issue_num')) == str(issue_id) or r.get('title') == issue_id), None)
        if import_data:

            v_res = import_data.get("verification_results") or {}
            v_res["historical_archive"] = {
                "csv_path": package["csv_path"],
                "tmcf_path": package["tmcf_path"],
                "gcs_csv_path": package.get("gcs_csv_path", ""),
                "gcs_tmcf_path": package.get("gcs_tmcf_path", ""),
                "count": package["count"],
                "run_name": package.get("run_name", ""),
                "generated_at": datetime.now(timezone.utc).isoformat()
            }
            db.update_verification_results(import_data.get("job_id"), v_res)
        
        return {
            "status": "success",
            "csv_path": package["csv_path"],
            "tmcf_path": package["tmcf_path"],
            "gcs_csv_path": package.get("gcs_csv_path", ""),
            "gcs_tmcf_path": package.get("gcs_tmcf_path", ""),
            "csv_preview": "\n".join(package["csv_content"].split("\n")[:10]),
            "tmcf_content": package["tmcf_content"],
            "count": package["count"],
            "run_name": package.get("run_name", ""),
            "commands": commands
        }
    except Exception as e:
        log.error(f"Error generating historical archive package: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/historical-archives/download/{issue_id}/{run_name}/{file_type}")
def download_archive_file(issue_id: str, run_name: str, file_type: str):
    if file_type not in ["csv", "tmcf"]:
        raise HTTPException(status_code=400, detail="Invalid file type. Must be csv or tmcf.")
        
    target_dir = os.path.join(HISTORICAL_ARCHIVES_DIR, issue_id, run_name)
    filename = f"historical_archive.{file_type}"
    local_path = os.path.join(target_dir, filename)
    
    if not os.path.exists(local_path):
        raise HTTPException(status_code=404, detail="Archive file not found. Please generate the historical files first.")
        
    return FileResponse(
        local_path,
        filename=filename,
        media_type="text/csv" if file_type == "csv" else "text/plain"
    )

@app.post("/api/historical-archives/verify/{issue_id}")
def verify_remediation(issue_id: str):
    try:
        return verify_remediation_union(issue_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        log.error(f"Failed to verify remediation: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Global in-memory job tracker for golden baseline generation
golden_generation_jobs = {}

def run_golden_generation_task(issue_id: str, run_folder: str):
    from .stage1_forensic.golden_generator import generate_goldens_in_gcs
    from .database import db
    
    golden_generation_jobs[issue_id] = {
        "status": "RUNNING",
        "error": None,
        "result": None
    }
    try:
        res = generate_goldens_in_gcs(issue_id, run_folder)
        golden_generation_jobs[issue_id] = {
            "status": "SUCCESS",
            "error": None,
            "result": res
        }
        # Update database with golden baselines status and GCS paths
        db.update_verification_results(issue_id, {
            "goldens_generation_status": "SUCCESS",
            "goldens_summary_path": res.get("summary_golden"),
            "goldens_observations_path": res.get("observations_golden"),
            "scanned_rows": res.get("scanned_rows"),
            "matched_rows": res.get("matched_rows"),
            "message": res.get("message")
        })
    except Exception as e:
        log.exception(f"Error in golden generation background task for issue {issue_id}")
        golden_generation_jobs[issue_id] = {
            "status": "FAILED",
            "error": str(e),
            "result": None
        }
        # Update database with failure status
        db.update_verification_results(issue_id, {
            "goldens_generation_status": "FAILED",
            "goldens_generation_error": str(e)
        })

@app.post("/api/historical-archives/generate-goldens/{issue_id}")
def trigger_golden_generation(issue_id: str, background_tasks: BackgroundTasks):
    from .database import db
    # 1. Fetch issue metadata
    all_issues = db.get_all_results()
    issue = next((item for item in all_issues if item.get('issue_id') == issue_id or item.get('job_id') == issue_id), None)
    if not issue:
        raise HTTPException(status_code=404, detail=f"Issue {issue_id} not found.")
        
    latest_run_folder = issue.get("latest_run_folder")
    if not latest_run_folder:
        raise HTTPException(status_code=400, detail="No run folder associated with this ingestion.")

    # 2. Check if job already running
    job = golden_generation_jobs.get(issue_id)
    if job and job["status"] == "RUNNING":
        return {"status": "RUNNING", "message": "Golden generation is already in progress."}

    # 3. Start background job
    golden_generation_jobs[issue_id] = {
        "status": "RUNNING",
        "error": None,
        "result": None
    }
    
    # Also update DB status to RUNNING
    db.update_verification_results(issue_id, {
        "goldens_generation_status": "RUNNING"
    })
    
    background_tasks.add_task(run_golden_generation_task, issue_id, latest_run_folder)
    return {"status": "RUNNING", "message": "Golden generation task started in background."}

@app.get("/api/historical-archives/generate-goldens/status/{issue_id}")
def get_golden_generation_status(issue_id: str):
    from .database import db
    # Check in-memory first
    job = golden_generation_jobs.get(issue_id)
    if job:
        return job

    # Fallback to DB
    all_issues = db.get_all_results()
    issue = next((item for item in all_issues if item.get('issue_id') == issue_id or item.get('job_id') == issue_id), None)
    if not issue:
        raise HTTPException(status_code=404, detail=f"Issue {issue_id} not found.")

    v_res = issue.get("verification_results") or {}
    status = v_res.get("goldens_generation_status")
    if status:
        return {
            "status": status,
            "error": v_res.get("goldens_generation_error"),
            "result": {
                "summary_golden": v_res.get("goldens_summary_path"),
                "observations_golden": v_res.get("goldens_observations_path"),
                "scanned_rows": v_res.get("scanned_rows"),
                "matched_rows": v_res.get("matched_rows"),
                "message": v_res.get("message")
            } if status == "SUCCESS" else None
        }

    return {"status": "NONE", "message": "No golden generation run found for this issue."}

@app.post("/api/issues/diagnose-logs/{job_id}")
async def manual_diagnose_logs(job_id: str):
    log.info(f"Manual log diagnosis requested for job: {job_id}")
    diag = await diagnose_job_logs_async(job_id)
    if not diag:
        raise HTTPException(status_code=404, detail="Logs could not be fetched or diagnosed.")
    
    # Save the diagnosis to the database record!
    async with db.write_lock:
        results = db.get_all_results()
        item = next((r for r in results if r.get('job_id') == job_id), None)
        if item:
            item['log_diagnosis'] = diag
            if hasattr(db, 'db'):
                db.db.collection(db.collection_name).document(job_id).set({"log_diagnosis": diag}, merge=True)
            elif hasattr(db, 'path'):
                data = db._read_data()
                for d in data:
                    if d.get('job_id') == job_id:
                        d['log_diagnosis'] = diag
                db._write_data(data)
                
            # Broadcast the update via SSE!
            await sse_manager.broadcast({
                "type": "stage_update",
                "import_name": item.get('title'),
                "status": item.get('status'),
                "latest_run_folder": item.get('latest_run_folder'),
                "job_id": job_id,
                "log_diagnosis": diag,
                "differ_summary": item.get('differ_summary'),
                "deleted_nodes_sample": item.get('deleted_nodes_sample'),
                "provenance_url": item.get('provenance_url'),
                "provenance_description": item.get('provenance_description'),
                "has_golden_config": item.get('has_golden_config')
            })
            
    return diag

@app.post("/api/admin/reset-database")
def reset_database():
    try:
        from .database import db
        import json
        import os
        import logging
        
        # Check if database is Firestore mode
        if hasattr(db, "db") and hasattr(db, "collection_name"):
            client = db.db
            collection = db.collection_name
            
            logger = logging.getLogger("db_reset")
            logger.info("Clearing failures collection in Firestore...")
            
            docs = client.collection(collection).list_documents()
            deleted_count = 0
            for doc in docs:
                doc.delete()
                deleted_count += 1
            
            logger.info(f"Deleted {deleted_count} documents.")
            
            # Read seed file
            seed_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "config/verification_results.json")
            logger.info(f"Reading configuration seed from: {seed_path}")
            
            if not os.path.exists(seed_path):
                raise FileNotFoundError(f"Configuration seed file {seed_path} not found.")
                
            with open(seed_path, 'r') as f:
                records = json.load(f)
                
            logger.info(f"Re-seeding database with {len(records)} records...")
            for r in records:
                doc_id = r.get("job_id")
                if not doc_id:
                    doc_id = f"custom-run-{r.get('issue_num')}"
                client.collection(collection).document(doc_id).set(r)
                
            return {"success": True, "message": f"Database reset successfully. Deleted {deleted_count} documents, re-seeded {len(records)} records."}
        else:
            return {"success": False, "message": "Database is not configured in Firestore mode. Skipping remote reset."}
            
    except Exception as e:
        log.error(f"Failed to reset database: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/healthz", status_code=status.HTTP_200_OK)
def healthz():
    return {"status": "ok"}

def resolve_details_from_job_logs(job_id: str) -> tuple[str, str] | None:
    import subprocess
    import json
    import re
    # Query logging for any GCS path string in this job's tasks logs
    job_cmd = [
        "gcloud", "logging", "read",
        f'resource.labels.job_id="{job_id}" AND "gs://"',
        f"--project={settings.project_id}",
        "--format=json",
        "--limit=50"
    ]
    try:
        jres = subprocess.run(job_cmd, capture_output=True, text=True, check=True)
        jlogs = json.loads(jres.stdout or "[]")
        for entry in jlogs:
            payload = entry.get('jsonPayload') or {}
            msg = payload.get('message') or entry.get('textPayload') or ""
            # Match path up to the timestamp subfolder to support arbitrary nesting
            match = re.search(r'(gs://[a-zA-Z0-9_.-]+/(?:scripts|statvar_imports)/(?:[^/\s\'",]+/)+)(\d{4}_\d{2}_\d{2}T\d{2}_\d{2}_\d{2}[\d_]*)', msg)
            if match:
                parent_path = match.group(1)
                timestamp_folder = match.group(2)
                folder = parent_path + timestamp_folder + "/"
                
                # Extract import name as the last segment of the parent path
                import_name = parent_path.strip('/').split('/')[-1]
                return import_name, folder
    except subprocess.CalledProcessError as e:
        log.error(f"Failed to resolve GCS folder for job {job_id}: {e.stderr or str(e)}")
    except Exception as e:
        log.error(f"Failed to resolve GCS folder for job {job_id}: {str(e)}")
    return None


def _fallback_unknown(rt) -> dict:
    from datetime import datetime, timezone
    job_age_minutes = None
    if rt:
        try:
            if isinstance(rt, str):
                clean_rt = rt.replace("Z", "+00:00")
                rt_dt = datetime.fromisoformat(clean_rt)
            elif isinstance(rt, datetime):
                rt_dt = rt
            else:
                rt_dt = None
            if rt_dt:
                if rt_dt.tzinfo is None:
                    rt_dt = rt_dt.replace(tzinfo=timezone.utc)
                job_age_minutes = (datetime.now(timezone.utc) - rt_dt).total_seconds() / 60.0
        except Exception:
            pass

    if job_age_minutes is not None and 0 <= job_age_minutes < 45:
        return {
            "classification": "PENDING_INDEXING",
            "error_message": "Logs are currently propagating in Cloud Logging. (The container recently completed execution or is finishing writing output.)",
            "recommendation": "Click 'Reanalyze' or wait a few moments for Cloud Logging to finish indexing.",
            "raw_logs_snippet": ""
        }

    return {
        "classification": "UNKNOWN",
        "error_message": "No container task logs were found. (The job may have timed out, exceeded memory limits, or failed during VM startup.)",
        "recommendation": "Verify VM startup logs or check if container resource limits are sufficient.",
        "raw_logs_snippet": ""
    }

def classify_log_entries(entries: list[dict], run_time: Optional[Any] = None) -> dict:
    import re

    if not entries:
        return _fallback_unknown(run_time)

    # Consolidated keywords for memory exhaustion
    memory_keywords = [
        "java.lang.outofmemoryerror",
        "outofmemoryerror",
        "oomkilled",
        "memory limit exceeded",
        "exceeded its allocated memory",
        "gc overhead limit exceeded",
        "gc overhead limit",
        "exit code 137"
    ]
    
    detected_classification = None
    detected_error = None
    recommendation = None
    raw_snippet = ""

    # Sort entries by timestamp to find latest logs or tracebacks
    entries.sort(key=lambda x: x.get("timestamp", ""))

    # First pass: check for memory limit or resource exhaustion signatures
    for entry in entries:
        payload = entry.get("jsonPayload") or {}
        raw_msg = payload.get("message") or entry.get("textPayload") or ""
        msg_lower = raw_msg.lower()
        if any(kw in msg_lower for kw in memory_keywords):
            detected_classification = "OUT_OF_MEMORY"
            detected_error = raw_msg.strip()
            recommendation = "Increase container memory limits in the import configuration manifest."
            raw_snippet = raw_msg
            break

    if detected_classification:
        return {
            "classification": detected_classification,
            "error_message": detected_error,
            "recommendation": recommendation,
            "raw_logs_snippet": raw_snippet
        }

    def is_ignorable_log_line(text: str) -> bool:
        t = text.strip()
        if t.startswith("Validation passed:") or t.startswith("Function start:") or t.startswith("Function end:"):
            return True
        if t.startswith("Import status summary:") or t.startswith("ImportStatusSummary(") or t.startswith("Import result: ExecutionResult"):
            return True
        if t.startswith("Import workflow completed") or t.startswith("Import Automation Success"):
            return True
        if t.startswith("Counters:") or t.startswith("Updating import staging version") or t.startswith("Merged validation config"):
            return True
        if t.startswith("Invoking validation script") or t.startswith("Resolved relative path"):
            return True
        return False

    error_lines = []
    validation_failures = []
    validation_runner_errors = []
    golden_missing_lines = []
    differ_missing_lines = []
    passed_validations = []
    validation_execution_lines = []
    traceback_found = False
    traceback_lines = []
    has_real_stderr_error = False
    real_stderr_error_line = None

    for entry in entries:
        payload = entry.get("jsonPayload") or {}
        raw_msg = payload.get("message") or entry.get("textPayload") or ""
        if not raw_msg:
            continue
            
        # Clean GKE wrapper prefix if present
        is_stderr = "Process stderr:" in raw_msg
        msg = raw_msg
        if "Process stderr:None: b\"" in raw_msg or "Process stderr:None: b'" in raw_msg or \
           "Process stdout:None: b\"" in raw_msg or "Process stdout:None: b'" in raw_msg:
            msg = raw_msg.split(" b\"", 1)[-1].split(" b'", 1)[-1].rstrip("\"'")
            msg = msg.replace("\\n", "\n").replace("\\t", "\t").strip()
            
        # Check if it has glog prefix and parse it
        glog_match = re.match(r'^([IWEF])\d{4}\s+\d{2}:\d{2}:\d{2}\.\d+\s+\d+\s+[^\]]+\]\s*(.*)$', msg, re.DOTALL)
        severity = None
        inner_content = msg
        if glog_match:
            severity = glog_match.group(1)
            inner_content = glog_match.group(2).strip()

        # Capture validation stage execution transcript
        if "Invoking import validation" in inner_content or "Invoking validation script" in inner_content or \
           "Validation passed:" in inner_content or "Differ summary is missing" in inner_content or \
           "Skipping differ tool" in inner_content or "Not able to find GCS file" in inner_content or \
           "ValidationRunner failed:" in inner_content or "Missing " in inner_content and " goldens in " in inner_content or \
           "Found " in inner_content and " missing golden records" in inner_content or \
           inner_content.startswith("FAILED:") or "validation: False" in inner_content:
            validation_execution_lines.append(inner_content)

        # Capture ValidationRunner failures
        if "ValidationRunner failed:" in inner_content:
            validation_runner_errors.append(inner_content)

        # Capture missing golden records
        if "Missing " in inner_content and " goldens in " in inner_content:
            golden_missing_lines.append(inner_content)
        elif "Found " in inner_content and " missing golden records" in inner_content:
            golden_missing_lines.append(inner_content)

        # Check for differ summary / missing baseline issues
        if "Differ summary is missing required field" in inner_content or \
           "Skipping differ tool due to missing" in inner_content or \
           ("Not able to find GCS file" in inner_content and "latest_version.txt" in inner_content):
            differ_missing_lines.append(inner_content)

        # Check for passed checks
        if inner_content.startswith("Validation passed:"):
            check_name = inner_content.split(":", 1)[-1].strip()
            if check_name and check_name not in passed_validations:
                passed_validations.append(check_name)

        # Check for ignorable noise
        if is_ignorable_log_line(inner_content):
            continue
            
        # Unwrap executor result wrapper if message is embedded
        if "message='Traceback" in inner_content or "message=\"Traceback" in inner_content:
            if "message='" in inner_content:
                inner_content = inner_content.split("message='", 1)[-1].rsplit("'", 1)[0]
            elif 'message="' in inner_content:
                inner_content = inner_content.split('message="', 1)[-1].rsplit('"', 1)[0]
            inner_content = inner_content.replace("\\n", "\n").replace("\\t", "\t").strip()

        # Catch traceback blocks
        if "Traceback (most recent call last):" in inner_content:
            traceback_found = True
            traceback_lines = [inner_content]
            continue
        elif traceback_found:
            traceback_lines.append(inner_content)
            trimmed = inner_content.strip()
            # If line is the final error line (e.g. ValueError: ..., Exception: ...)
            if (re.match(r'^[A-Za-z0-9_.]*(?:Error|Exception|Interrupt|Exit):', trimmed) or \
               ("Error:" in trimmed or "Exception:" in trimmed)) and not trimmed.startswith("raise "):
                traceback_found = False
            continue

        # Check for specific validation failure lines
        if "deleted records, which is over the threshold" in inner_content or \
           "missing references, which is over the threshold" in inner_content or \
           "lint errors, which is over the threshold" in inner_content or \
           "conflicting properties" in inner_content or \
           inner_content.startswith("FAILED: check_"):
            validation_failures.append(inner_content)
            continue
            
        # Identify real errors
        if severity in ["E", "F"]:
            error_lines.append(inner_content)
            if is_stderr:
                has_real_stderr_error = True
                real_stderr_error_line = inner_content
        elif severity != "I":
            inner_lower = inner_content.lower()
            if "client error" in inner_lower or "httperror" in inner_lower or "exception:" in inner_lower or "error:" in inner_lower or "failed:" in inner_lower:
                error_lines.append(inner_content)
                if is_stderr:
                    has_real_stderr_error = True
                    real_stderr_error_line = inner_content

    diagnosis = ""
    # 1. Look for traceback first
    if traceback_lines:
        full_tb = "\n".join(traceback_lines)
        # Find the last meaningful error line from the traceback
        tb_lines = [line.strip() for line in full_tb.strip().split("\n") if line.strip()]
        # Prefer specific root causes over generic executor wrapper ExecutionError
        specific_error = None
        for line in tb_lines:
            if ("CalledProcessError:" in line or "FileNotFoundError:" in line or "ImportError:" in line or 
                "ModuleNotFoundError:" in line or "ValueError:" in line or "KeyError:" in line or 
                "TypeError:" in line or "ZeroDivisionError:" in line or "AttributeError:" in line or 
                "IndexError:" in line or "Error:" in line or "Exception:" in line):
                if not line.endswith("ExecutionError"):
                    specific_error = line
        if specific_error:
            last_line = specific_error
        else:
            for line in reversed(tb_lines):
                if "Error:" in line or "Exception:" in line or "Error" in line:
                    last_line = line
                    break
        
        # Clean verbose command paths if it's a CalledProcessError
        if "CalledProcessError:" in last_line:
            status_match = re.search(r'returned (?:non-zero )?exit status (\d+)', last_line)
            status_code = status_match.group(1) if status_match else "1"
            
            script_match = re.search(r'([a-zA-Z0-9_\-]+\.py)', last_line)
            jar_match = re.search(r'import-tool\.jar', last_line)
            sh_match = re.search(r'([a-zA-Z0-9_\-]+\.sh)', last_line)
            
            if script_match:
                last_line = f"subprocess.CalledProcessError: Command '{script_match.group(1)}' returned exit status {status_code}."
            elif jar_match:
                tool_act = "genmcf" if "genmcf" in last_line else "import-tool"
                csv_match = re.search(r'([a-zA-Z0-9_\-]+\.csv)', last_line)
                target = f" on {csv_match.group(1)}" if csv_match else ""
                last_line = f"subprocess.CalledProcessError: Import Tool ({tool_act}) failed with exit status {status_code}{target}."
            elif sh_match:
                last_line = f"subprocess.CalledProcessError: Command '{sh_match.group(1)}' returned exit status {status_code}."

        diagnosis = last_line
        detected_classification = "PYTHON_TRACEBACK"
        recommendation = "Inspect the Python exception to fix the script error."
        raw_snippet = full_tb
        
    # 2. Look for schema / validation failures (deleted records, missing refs, lint errors, goldens, validation runner)
    if not diagnosis and (validation_failures or validation_runner_errors or differ_missing_lines or passed_validations or golden_missing_lines):
        valid_failures = [vf for vf in validation_failures if not vf.startswith("FAILED: None") and vf != "FAILED: None"]
        if valid_failures:
            # If multiple threshold failure lines occurred (e.g. both deleted records and missing refs), combine them
            threshold_failures = [vf for vf in valid_failures if "over the threshold" in vf]
            if threshold_failures:
                unique_tf = []
                for tf in threshold_failures:
                    if tf not in unique_tf:
                        unique_tf.append(tf)
                diagnosis = " ".join(unique_tf)
            elif any(vf.startswith("FAILED:") for vf in valid_failures):
                all_failed_rules = []
                for vf in valid_failures:
                    if vf.startswith("FAILED:") or vf.startswith("FAILED :"):
                        raw_rules = vf.split(":", 1)[1]
                        parts = [p.strip() for p in raw_rules.split(",") if p.strip() and p.strip().lower() != "none"]
                        for p in parts:
                            if p not in all_failed_rules:
                                all_failed_rules.append(p)
                if all_failed_rules:
                    diagnosis = "FAILED: " + ", ".join(all_failed_rules)
                else:
                    diagnosis = valid_failures[-1]
            else:
                diagnosis = valid_failures[-1]

            detected_classification = "SCHEMA_VALIDATION_ERROR"
            recommendation = "Resolve conflicting node properties or validation rules in the import schema."
            raw_snippet = "\n".join(validation_execution_lines) if validation_execution_lines else diagnosis
        elif validation_runner_errors:
            vr_err = validation_runner_errors[-1]
            if "A validation rule requires the 'lint' data source" in vr_err:
                diagnosis = "ValidationRunner: Missing required lint report (--lint_report file not provided or report.json missing)."
                recommendation = "Enable genmcf lint reporting or remove lint check from validation_config.json."
            else:
                diagnosis = vr_err
                recommendation = "Review the validation runner configuration."
            detected_classification = "SCHEMA_VALIDATION_ERROR"
            raw_snippet = "\n".join(validation_execution_lines) if validation_execution_lines else diagnosis
        elif differ_missing_lines:
            diagnosis = f"Baseline missing: {differ_missing_lines[-1]}"
            if passed_validations:
                diagnosis += f" (Passed checks: {', '.join(passed_validations)})"
            detected_classification = "SCHEMA_VALIDATION_ERROR"
            recommendation = "Initial run missing prior production baseline on GCS. Add validation_config.json override to bypass baseline deletion checks."
            raw_snippet = "\n".join(validation_execution_lines) if validation_execution_lines else diagnosis
        elif golden_missing_lines:
            diagnosis = golden_missing_lines[-1]
            detected_classification = "SCHEMA_VALIDATION_ERROR"
            recommendation = "Update golden test baselines or verify import transformation logic."
            raw_snippet = "\n".join(validation_execution_lines) if validation_execution_lines else diagnosis
        elif passed_validations:
            diagnosis = f"Validation assertions failed (Passed checks: {', '.join(passed_validations)})"
            detected_classification = "SCHEMA_VALIDATION_ERROR"
            recommendation = "Review validation assertions and TMCF mappings."
            raw_snippet = "\n".join(validation_execution_lines) if validation_execution_lines else diagnosis

    # 3. Look for HTTP client errors
    if not diagnosis:
        http_errors = [line for line in error_lines if "httperror" in line.lower() or "client error" in line.lower() or "404" in line or "502" in line or "503" in line]
        if http_errors:
            diagnosis = http_errors[-1]
            detected_classification = "HTTP_ERROR"
            recommendation = "Verify the external URL format and upstream server status."
            raw_snippet = http_errors[-1]

    # 4. Look for real stderr error line
    if not diagnosis and has_real_stderr_error:
        diagnosis = real_stderr_error_line
        detected_classification = "STDERR_ERROR"
        recommendation = "Review the process error details below."
        raw_snippet = real_stderr_error_line
        
    # 5. Fall back to general error lines
    if not diagnosis and error_lines:
        diagnosis = error_lines[-1]
        detected_classification = "GENERAL_ERROR"
        recommendation = "Review the general log details below for troubleshooting."
        raw_snippet = error_lines[-1]

    if diagnosis:
        return {
            "classification": detected_classification,
            "error_message": diagnosis.strip(),
            "recommendation": recommendation,
            "raw_logs_snippet": raw_snippet.strip()
        }
        
    return _fallback_unknown(run_time)

async def diagnose_job_logs_async(job_id: str, run_time: Optional[Any] = None) -> dict | None:
    import json
    import asyncio
    from datetime import datetime, timedelta
    from .settings import settings

    existing_doc_error = None
    existing_doc_diag = None
    latest_run_folder = None
    doc_data = {}

    job_name = None
    try:
        from .database import get_db
        db = get_db()
        doc = db.db.collection(db.collection_name).document(job_id).get()
        if doc.exists:
            doc_data = doc.to_dict() or {}
            if run_time is None:
                run_time = doc_data.get("run_time") or doc_data.get("timestamp")
            job_name = doc_data.get("job_name")
            latest_run_folder = doc_data.get("latest_run_folder")
            existing_doc_error = doc_data.get("error_message")
            existing_doc_diag = doc_data.get("log_diagnosis")
    except Exception:
        pass

    # Use bounded time filter around run_time if available for ultra-fast query execution
    history_limit = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
    time_filter = f'timestamp >= "{history_limit}"'
    if run_time:
        try:
            if isinstance(run_time, str):
                rt_clean = run_time.replace("Z", "+00:00")
                run_dt = datetime.fromisoformat(rt_clean).replace(tzinfo=None)
            elif isinstance(run_time, datetime):
                run_dt = run_time.replace(tzinfo=None)
            else:
                run_dt = None
            if run_dt:
                start_ts = (run_dt - timedelta(hours=6)).isoformat() + "Z"
                end_ts = (run_dt + timedelta(hours=36)).isoformat() + "Z"
                time_filter = f'timestamp >= "{start_ts}" AND timestamp <= "{end_ts}"'
        except Exception:
            pass

    target_ids = {job_id}
    if job_name:
        target_ids.add(job_name)
    
    id_clauses = []
    for jid in target_ids:
        id_clauses.extend([
            f'labels.task_group_name:"{jid}"',
            f'labels.job_uid="{jid}"',
            f'resource.labels.job_id="{jid}"'
        ])
    id_filter = " OR ".join(id_clauses)

    log_filter = (
        f'(logName="projects/{settings.project_id}/logs/batch_task_logs" OR logName="projects/{settings.project_id}/logs/batch_agent_logs" OR resource.type="batch.googleapis.com/Job") AND '
        f'({id_filter}) AND '
        f'{time_filter}'
    )
    cmd = [
        "gcloud", "logging", "read",
        log_filter,
        f"--project={settings.project_id}",
        "--format=json",
        "--limit=100"
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=25.0)
            if proc.returncode == 0 and stdout:
                entries = json.loads(stdout.decode('utf-8') or "[]")
            else:
                entries = []
        except asyncio.TimeoutError:
            log.warning(f"gcloud logging read timed out for job_id: {job_id} in project {settings.project_id}")
            try:
                proc.kill()
            except Exception:
                pass
            entries = []
    except Exception as e:
        log.warning(f"Failed to query logs for job {job_id} in project {settings.project_id}: {e}")
        entries = []

    if not entries:
        if existing_doc_diag and existing_doc_diag.get("classification") and existing_doc_diag.get("classification") not in ["UNKNOWN", "PENDING_INDEXING"]:
            return existing_doc_diag
        if latest_run_folder:
            try:
                from .stage1_forensic.validation_reader import fetch_validation_failures
                gcs_diag = fetch_validation_failures(latest_run_folder)
                if gcs_diag:
                    return gcs_diag
            except Exception as e:
                log.warning(f"Failed to fetch GCS validation failures for {job_id}: {e}")
        if existing_doc_error and not existing_doc_error.startswith("No container task logs"):
            return classify_log_entries([{"textPayload": existing_doc_error}], run_time=run_time)
        return _fallback_unknown(run_time)

    return classify_log_entries(entries, run_time=run_time)


def classify_sync_error(stderr_msg: str) -> str:
    if not stderr_msg:
        return "An unexpected error occurred."
    msg_lower = stderr_msg.lower()
    if "permission denied" in msg_lower or "403" in msg_lower or "unauthorized" in msg_lower:
        return "Access denied."
    if "api is not enabled" in msg_lower or "not enabled" in msg_lower:
        return "Service error."
    if "timeout" in msg_lower or "timed out" in msg_lower or "connection" in msg_lower:
        return "Network timeout."
    if "not found" in msg_lower or "404" in msg_lower:
        return "Logs not found."
    return "An unexpected error occurred."

async def check_job_has_logs(job_id: str, retries: int = 3) -> Optional[bool]:
    from .settings import settings
    import json
    log_filter = (
        f'(logName="projects/{settings.project_id}/logs/batch_task_logs" OR logName="projects/{settings.project_id}/logs/batch_agent_logs") AND '
        f'(labels.task_group_name:"{job_id}" OR labels.job_uid="{job_id}" OR resource.labels.job_id="{job_id}")'
    )
    cmd = [
        "gcloud", "logging", "read",
        log_filter,
        f"--project={settings.project_id}",
        "--format=json",
        "--limit=1"
    ]
    for attempt in range(retries):
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            try:
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=8.0)
            except asyncio.TimeoutError:
                log.warning(f"Sync: gcloud logging read timed out while checking logs for {job_id} (Attempt {attempt+1}/{retries})")
                try:
                    proc.kill()
                    await proc.wait()
                except Exception:
                    pass
                if attempt == retries - 1:
                    return None
                await asyncio.sleep(1.0 * (attempt + 1))
                continue
                
            if proc.returncode != 0:
                log.warning(f"Sync: gcloud logging read returned non-zero code {proc.returncode} for {job_id} (Attempt {attempt+1}/{retries}). Stderr: {stderr.decode('utf-8')}")
                if attempt == retries - 1:
                    return None
                await asyncio.sleep(1.0 * (attempt + 1))
                continue
                
            entries = json.loads(stdout.decode('utf-8') or "[]")
            return len(entries) > 0
        except Exception as e:
            log.error(f"Sync: Exception while checking logs for {job_id}: {str(e)} (Attempt {attempt+1}/{retries})")
            if attempt == retries - 1:
                return None
            await asyncio.sleep(1.0 * (attempt + 1))
    return None

async def sync_batch_jobs_lifecycle(existing_jobs: dict) -> list:
    from .settings import settings
    from .database import db
    from .models import FailureEvent, REPROCESSABLE_STATUSES
    from datetime import datetime, timedelta, timezone
    import json
    
    cmd = [
        "gcloud", "batch", "jobs", "list",
        f"--project={settings.project_id}",
        "--format=json"
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            log.error(f"Sync: Failed to list Batch jobs: {stderr.decode('utf-8')}")
            return []
        batch_jobs = json.loads(stdout.decode('utf-8') or "[]")
    except Exception as e:
        log.error(f"Sync: Failed to query Batch jobs list: {str(e)}")
        return []
    history_limit = datetime.now(timezone.utc) - timedelta(days=settings.sync_window_days)

    
    # Track active batch job IDs from the API response
    active_api_job_ids = set()
    api_jobs_by_id = {}
    
    for job in batch_jobs:
        name = job.get("name", "")
        job_id = name.split("/")[-1] if "/" in name else ""
        if not job_id:
            continue
        api_jobs_by_id[job_id] = job
        state = job.get("status", {}).get("state", "UNKNOWN")
        if state in ["RUNNING", "SCHEDULED"]:
            active_api_job_ids.add(job_id)

    # Filter and prepare jobs
    failed_batch_jobs = []

    for job in batch_jobs:
        create_time_str = job.get("createTime")
        if not create_time_str:
            continue
        create_time = datetime.fromisoformat(create_time_str.replace("Z", "+00:00")).replace(tzinfo=None)
        if create_time < history_limit:
            continue
            
        name = job.get("name", "")
        parts = name.split("/")
        project_id = parts[1] if len(parts) > 1 and parts[0] == "projects" else settings.project_id
        region = parts[3] if len(parts) > 3 and parts[2] == "locations" else "us-central1"
        job_id = parts[5] if len(parts) > 5 and parts[4] == "jobs" else (parts[-1] if "/" in name else name)
        if not job_id or job_id.startswith("test-curl"):
            continue
            
        state = job.get("status", {}).get("state", "UNKNOWN")
        
        import_name = None
        try:
            task_groups = job.get("taskGroups", [])
            if task_groups:
                runnables = task_groups[0].get("taskSpec", {}).get("runnables", [])
                if runnables:
                    commands = runnables[0].get("container", {}).get("commands", [])
                    for cmd_arg in commands:
                        if "--import_name=" in cmd_arg:
                            raw_import = cmd_arg.split("=")[-1]
                            import_name = raw_import.split(":")[-1].split("/")[-1]
                            break
        except Exception:
            pass
            
        if not import_name:
            import_name = job_id.split("-")[0]
            
        if state in ["RUNNING", "SCHEDULED"]:
            existing_record = existing_jobs.get(job_id)
            if not existing_record:
                log.info(f"Sync: Discovered active running Batch job {job_id} in {region}. Adding RUNNING status.")
                event = FailureEvent(
                    job_id=job_id,
                    job_uid=job_id,
                    import_name=import_name,
                    stage_name="INIT",
                    status="FAILURE",
                    message="Pipeline execution is in progress...",
                    timestamp=create_time,
                    gcs_path=None,
                    job_name=job_id,
                    region=region,
                    project_id=project_id
                )
                db.save_or_update_result(
                    event, "RUNNING", None, None,
                    provenance_url=None,
                    provenance_description=None
                )
        elif state == "FAILED":
            existing_record = existing_jobs.get(job_id)
            if not existing_record or existing_record.get('status') in REPROCESSABLE_STATUSES:
                failed_batch_jobs.append({
                    'job_id': job_id,
                    'job_name': job_id,
                    'import_name': import_name,
                    'create_time': create_time,
                    'region': region,
                    'project_id': project_id
                })
        elif state == "SUCCEEDED":
            existing_record = existing_jobs.get(job_id)
            if existing_record and existing_record.get('status') in ["RUNNING", "PENDING", "PENDING_SYNC"]:
                # Check if a real run document for this import already exists with full GCS data
                has_real_run = any(
                    doc.get('title') == import_name and doc.get('latest_run_folder') and doc.get('job_id') != job_id
                    for doc in existing_jobs.values()
                )
                if has_real_run:
                    log.info(f"Sync: Job {job_id} ({import_name}) completed and real GCS run exists. Removing temporary placeholder.")
                    if hasattr(db, 'db'):
                        db.db.collection(db.collection_name).document(job_id).delete()
                else:
                    log.info(f"Sync: Discovered previously running job {job_id} has SUCCEEDED. Resolving in DB.")
                    event = FailureEvent(
                        job_id=job_id,
                        job_uid=job_id,
                        import_name=import_name,
                        stage_name="FINISH",
                        status="SUCCESS",
                        message="Pipeline completed successfully (verified via Batch API).",
                        timestamp=create_time,
                        gcs_path=None,
                        job_name=job_id,
                        region=region,
                        project_id=project_id
                    )
                    db.save_or_update_result(
                        event, "RESOLVED", None, None,
                        provenance_url=None,
                        provenance_description=None
                    )
        elif state in ["CANCELLED", "BLOCKED"]:
            existing_record = existing_jobs.get(job_id)
            if existing_record and existing_record.get('status') in ["RUNNING", "PENDING", "PENDING_SYNC"]:
                log.info(f"Sync: Discovered previously running job {job_id} was {state}. Resolving in DB.")
                event = FailureEvent(
                    job_id=job_id,
                    job_uid=job_id,
                    import_name=import_name,
                    stage_name="INIT",
                    status="FAILURE",
                    message=f"Pipeline execution was {state.lower()} (verified via Batch API).",
                    timestamp=create_time,
                    gcs_path=None,
                    job_name=job_id,
                    region=region,
                    project_id=project_id
                )
                db.save_or_update_result(
                    event, "RESOLVED", None, None,
                    provenance_url=None,
                    provenance_description=None
                )

    # 2. Re-resolve jobs that are marked RUNNING in our DB but are no longer active in Cloud Batch
    for db_job_id, db_job_info in existing_jobs.items():
        if db_job_info.get('status') == 'RUNNING':
            # Check if this job is not active in the current API response
            if db_job_id not in active_api_job_ids:
                api_job = api_jobs_by_id.get(db_job_id)
                if api_job:
                    state = api_job.get("status", {}).get("state", "UNKNOWN")
                    log.info(f"Sync: Job {db_job_id} is marked RUNNING in DB but Cloud Batch state is {state}. Updating to RESOLVED.")
                else:
                    log.info(f"Sync: Job {db_job_id} is marked RUNNING in DB but is no longer in Cloud Batch history. Updating to RESOLVED.")
                
                import_name = db_job_id.split("-")[0]
                event = FailureEvent(
                    job_id=db_job_id,
                    job_uid=db_job_id,
                    import_name=import_name,
                    stage_name="FINISH",
                    status="SUCCESS",
                    message=f"Pipeline execution ended (Batch state: {state if api_job else 'PURGED'}).",
                    timestamp=datetime.now(timezone.utc) - timedelta(days=1),
                    gcs_path=None,
                    job_name=db_job_id,
                    region=db_job_info.get('region'),
                    project_id=db_job_info.get('project_id', settings.project_id)
                )
                db.save_or_update_result(
                    event, "RESOLVED", None, None,
                    provenance_url=None,
                    provenance_description=None
                )

    return failed_batch_jobs

sync_lock = asyncio.Lock()

async def run_sync_worker():
    if sync_lock.locked():
        log.warning("Sync: Worker already active. Skipping duplicate run.")
        return
        
    async with sync_lock:
        await _run_sync_worker_impl()

async def _run_sync_worker_impl():
    from .database import db
    from .models import FailureEvent, REPROCESSABLE_STATUSES
    from .orchestrator import run_stages
    from .sse import sse_manager
    import asyncio
    import json
    from datetime import datetime, timedelta, timezone
    import traceback
    import re
    
    log.info("Starting background logs sync worker...")
    await sse_manager.broadcast({"type": "sync_start"})
    
    try:
        # 1. Get existing records
        records = db.get_all_results()
        existing_jobs = {
            item.get('job_id'): {
                'status': item.get('status'),
                'job_name': item.get('job_name')
            }
            for item in records if item.get('job_id')
        }
        existing_records_by_title = {
            item.get('title'): {
                'status': item.get('status'),
                'job_name': item.get('job_name'),
                'job_id': item.get('job_id'),
                'run_time': item.get('run_time')
            }
            for item in records if item.get('title')
        }
        
        # 2. Sync active and silent Batch jobs first
        failed_batch_jobs = await sync_batch_jobs_lifecycle(existing_jobs)
        
        # 3. Query Cloud Logging for failed jobs and successful completions in the last configured days
        from .settings import settings
        
        last_sync = None
        try:
            last_sync_str = db.get_last_sync_time()
            if last_sync_str:
                last_sync = datetime.fromisoformat(last_sync_str.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception as ex:
            log.warning(f"Sync: Failed to read last sync time, defaulting to sync window window: {ex}")
            
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        default_limit = now - timedelta(days=settings.sync_window_days)
        
        if last_sync and last_sync > default_limit:
            # Overlap by 2 hours to cover ingest latency / clock drifts
            sync_start = last_sync - timedelta(hours=2)
            if sync_start < default_limit:
                sync_start = default_limit
            log.info(f"Sync: Using incremental sync starting from {sync_start.isoformat()}Z (last sync was {last_sync_str})")
        else:
            sync_start = default_limit
            log.info(f"Sync: Using full sync window starting from {sync_start.isoformat()}Z")

        sync_limit = sync_start.isoformat() + "Z"
        log_filter = (
            f'logName="projects/{settings.project_id}/logs/batch_task_logs" AND '
            '(jsonPayload.status="FAILURE" OR (jsonPayload.status="SUCCESS" AND jsonPayload.stage_name="FINISH")) AND '
            f'timestamp >= "{sync_limit}"'
        )
        cmd = [
            "gcloud", "logging", "read",
            log_filter,
            f"--project={settings.project_id}",
            "--format=json"
        ]
        
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()
        
        if proc.returncode != 0:
            stderr_msg = stderr.decode('utf-8', errors='ignore')
            user_msg = classify_sync_error(stderr_msg)
            debug_details = f"Command {' '.join(cmd)} returned {proc.returncode}. Stderr: {stderr_msg}"
            log.error(f"Sync failed during logging query: {debug_details}")
            await sse_manager.broadcast({
                "type": "sync_error",
                "message": f"Sync failed: {user_msg}",
                "debug_details": debug_details
            })
            return
        logs = json.loads(stdout.decode('utf-8') or "[]")
        # Reverse logs to process them chronologically (oldest first).
        # This ensures historical failures are imported before successes, resolving them correctly on a blank database.
        logs.reverse()
    except Exception as e:
        error_msg = f"Sync failed during logging query: {str(e)}"
        log.error(error_msg)
        log.error(traceback.format_exc())
        await sse_manager.broadcast({
            "type": "sync_error",
            "message": "Sync failed: An unexpected error occurred while querying logs.",
            "debug_details": error_msg
        })
        return
        
    # Identify new unique job IDs
    new_job_ids = []
    seen_jobs = set()
    for entry in logs:
        resource = entry.get('resource', {})
        labels = resource.get('labels', {})
        job_id = labels.get('job_id')
        if job_id and job_id not in seen_jobs:
            existing_record = existing_jobs.get(job_id)
            if existing_record:
                status_in_db = existing_record.get('status')
                job_name_in_db = existing_record.get('job_name')
                if status_in_db and status_in_db not in REPROCESSABLE_STATUSES and job_name_in_db:
                    continue
            seen_jobs.add(job_id)
            new_job_ids.append(job_id)

    # Merge failed Batch jobs that did not emit a structured status log into logs
    for fj in failed_batch_jobs:
        f_jid = fj['job_id']
        if f_jid not in seen_jobs:
            seen_jobs.add(f_jid)
            new_job_ids.append(f_jid)
            synthetic_entry = {
                'resource': {
                    'labels': {
                        'job_id': f_jid,
                        'location': fj.get('region', 'us-central1'),
                        'resource_container': f"projects/{fj.get('project_id', settings.project_id)}"
                    }
                },
                'labels': {
                    'task_group_name': f"projects/{fj.get('project_id', settings.project_id)}/locations/{fj.get('region', 'us-central1')}/jobs/{f_jid}"
                },
                'timestamp': fj['create_time'].isoformat() + "Z",
                'jsonPayload': {
                    'status': 'FAILURE',
                    'stage_name': 'INIT',
                    'import_name': fj['import_name'],
                    'message': 'Ingestion pipeline failed during task execution.'
                }
            }
            logs.append(synthetic_entry)

    # 4. Bulk resolve GCS details for all new jobs asynchronously in batches (in parallel)
    job_details_map = {}
    errors_count = 0
    
    async def fetch_batch_details(batch):
        batch_map = {}
        nonlocal errors_count
        job_conditions = " OR ".join([f'resource.labels.job_id="{jid}"' for jid in batch])
        bulk_filter = f'resource.type="batch.googleapis.com/Job" AND "gs://" AND ({job_conditions})'
        
        bulk_cmd = [
            "gcloud", "logging", "read",
            bulk_filter,
            f"--project={settings.project_id}",
            "--format=json",
            "--limit=300"
        ]
        try:
            log.info(f"Sync: Running bulk GCS path query for {len(batch)} jobs...")
            proc = await asyncio.create_subprocess_exec(
                *bulk_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            try:
                b_stdout, b_stderr = await asyncio.wait_for(proc.communicate(), timeout=20.0)
            except asyncio.TimeoutError:
                log.warning("gcloud logging read timed out for bulk GCS path query")
                try:
                    proc.kill()
                    await proc.wait()
                except Exception:
                    pass
                return batch_map
            if proc.returncode != 0:
                log.error(f"Sync: Bulk path query command failed: {b_stderr.decode('utf-8')}")
                errors_count += len(batch)
                return batch_map
            jlogs = json.loads(b_stdout.decode('utf-8') or "[]")
            for jentry in jlogs:
                j_resource = jentry.get('resource', {})
                j_labels = j_resource.get('labels', {})
                jid = j_labels.get('job_id')
                if not jid or jid in batch_map:
                    continue
                
                j_payload = jentry.get('jsonPayload') or {}
                msg = j_payload.get('message') or jentry.get('textPayload') or ""
                
                match = re.search(r'(gs://[a-zA-Z0-9_.-]+/(?:scripts|statvar_imports)/(?:[^/\s\'",]+/)+)(\d{4}_\d{2}_\d{2}T\d{2}_\d{2}_\d{2}[\d_]*)', msg)
                if match:
                    parent_path = match.group(1)
                    timestamp_folder = match.group(2)
                    folder = parent_path + timestamp_folder + "/"
                    import_name = parent_path.strip('/').split('/')[-1]
                    batch_map[jid] = (import_name, folder)
        except Exception as e:
            log.error(f"Sync: Bulk resolution failed for batch: {str(e)}")
            errors_count += len(batch)
        return batch_map


    if new_job_ids:
        batch_size = 50
        batches = [new_job_ids[i:i+batch_size] for i in range(0, len(new_job_ids), batch_size)]
        tasks = [fetch_batch_details(b) for b in batches]
        results = await asyncio.gather(*tasks)
        for res in results:
            job_details_map.update(res)

    # 5. Process the failures with resolved details in parallel
    discovered_count = 0
    unique_logs = []
    processed_jobs = set()
    for entry in logs:
        resource = entry.get('resource', {})
        labels = resource.get('labels', {})
        job_id = labels.get('job_id')
        if not job_id or job_id in processed_jobs:
            continue
        processed_jobs.add(job_id)
        unique_logs.append(entry)

    sem = asyncio.Semaphore(3)
    completed_tasks = 0
    completed_tasks_lock = asyncio.Lock()

    async def process_log_entry(entry):
        nonlocal discovered_count, errors_count, completed_tasks
        async with sem:
            job_id = "N/A"
            try:
                resource = entry.get('resource', {})
                labels = resource.get('labels', {})
                job_id = labels.get('job_id')
                
                payload = entry.get('jsonPayload') or {}
                event_status = payload.get('status', 'FAILURE')
                event_stage = payload.get('stage_name', 'VALIDATION')
                
                import_name = None
                folder = None
                
                resolved = job_details_map.get(job_id)
                if resolved:
                    import_name, folder = resolved
                else:
                    import_name = payload.get('import_name')
                    if not import_name:
                        msg = payload.get('message') or entry.get('textPayload') or ""
                        if "Import: " in msg:
                            try:
                                import_name = msg.split("Import: ")[1].split(",")[0].strip()
                            except Exception:
                                pass
                    
                if not import_name:
                    log.warning(f"Sync: Could not resolve import name for job {job_id}. Skipping.")
                    return
                    
                async with completed_tasks_lock:
                    completed_tasks += 1
                    curr_num = completed_tasks
                
                await sse_manager.broadcast({
                    "type": "sync_progress",
                    "current": curr_num,
                    "total": len(unique_logs),
                    "import_name": import_name
                })
                
                timestamp_str = entry.get('timestamp', 'N/A')
                if timestamp_str != 'N/A':
                    try:
                        parsed_ts = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    except Exception:
                        parsed_ts = datetime.now(timezone.utc)
                else:
                    parsed_ts = datetime.now(timezone.utc)

                existing_record = existing_records_by_title.get(import_name)
                if existing_record:
                    db_run_time_str = existing_record.get('run_time')
                    db_latest_folder = existing_record.get('latest_run_folder')
                    if db_run_time_str:
                        try:
                            db_ts = datetime.fromisoformat(db_run_time_str.replace('Z', '+00:00'))
                            if parsed_ts < db_ts:
                                return
                            elif parsed_ts == db_ts and db_latest_folder:
                                return
                        except Exception:
                            pass

                if event_status == "FAILURE":
                    pass
                elif event_status == "SUCCESS" and event_stage == "FINISH":
                    if not existing_record or existing_record.get('status') == 'RESOLVED':
                        return
                else:
                    return
                        
                log.info(f"Sync discovered event: {import_name} (Job ID={job_id}, Status={event_status})")
                
                msg = payload.get('message') or entry.get('textPayload') or "Ingestion pipeline run update"
                diag_data = None
                if event_status == "FAILURE" and not folder:
                    log.info(f"Sync: Diagnosing ingestion failure for job {job_id}...")
                    diag_data = await diagnose_job_logs_async(job_id, run_time=parsed_ts)
                    if diag_data:
                        msg = diag_data.get("error_message") or msg
                
                log_labels = entry.get('labels') or {}
                task_group_name = log_labels.get('task_group_name')
                job_name = None
                if task_group_name:
                    parts = task_group_name.split('/jobs/')
                    if len(parts) > 1:
                        job_name = parts[1].split('/')[0]
                    
                proj = labels.get('resource_container')
                if proj and proj.startswith("projects/"):
                    proj = proj.replace("projects/", "")
                    
                loc = labels.get('location')
                region = loc
                if loc:
                    parts = loc.split('-')
                    if len(parts) > 2:
                        region = "-".join(parts[:-1])
                    
                event = FailureEvent(
                    job_id=job_id,
                    job_uid=job_id,
                    import_name=import_name,
                    stage_name=event_stage,
                    status=event_status,
                    message=msg.strip(),
                    timestamp=parsed_ts,
                    gcs_path=folder,
                    job_name=job_name,
                    region=region,
                    project_id=proj,
                    log_diagnosis=diag_data
                )
                await run_stages(event, update_watermark=False)
                async with completed_tasks_lock:
                    if event_status == "SUCCESS":
                        if import_name in existing_records_by_title:
                            existing_records_by_title[import_name]["status"] = "RESOLVED"
                    else:
                        existing_jobs[job_id] = {
                            "status": event_status,
                            "job_name": job_name
                        }
                        existing_records_by_title[import_name] = {
                            "status": event_status,
                            "job_name": job_name,
                            "job_id": job_id,
                            "run_time": timestamp_str
                        }
                discovered_count += 1
                
            except Exception as e:
                errors_count += 1
                err_details = f"Failed to parse and sync job {job_id}: {str(e)}"
                log.error(err_details)
                log.error(traceback.format_exc())
                await sse_manager.broadcast({
                    "type": "sync_error",
                    "message": f"Sync failed for run {job_id}.",
                    "debug_details": err_details
                })
            finally:
                cleanup_memory()

    if unique_logs:
        sync_tasks = [process_log_entry(entry) for entry in unique_logs]
        await asyncio.gather(*sync_tasks)
            
    # 5. Process any remaining PENDING or UNKNOWN diagnosis jobs in the database (Automated Self-Healing Triage)
    from .stage1_forensic.validation_reader import fetch_validation_failures

    current_records = db.get_all_results()
    heal_candidates = []
    for item in current_records:
        if item.get('status') == 'PENDING':
            heal_candidates.append(item)
            continue
        diag = item.get('log_diagnosis') or {}
        classification = diag.get('classification') or 'UNKNOWN'
        err_msg = diag.get('error_message') or item.get('error_message') or ''
        if classification in ['UNKNOWN', 'PENDING_INDEXING'] or 'no container task logs' in err_msg.lower():
            heal_candidates.append(item)
    
    async def process_heal_job(p_job):
        nonlocal discovered_count, errors_count

        p_job_id = p_job.get('job_id')
        p_title = p_job.get('title')
        p_run_time_str = p_job.get('run_time')
        p_error_msg = p_job.get('error_message') or "Ingestion pipeline run update"
        p_job_name = p_job.get('job_name')
        p_run_folder = p_job.get('latest_run_folder')
        
        if not p_job_id or not p_title:
            return
            
        try:
            if p_run_time_str:
                p_ts = datetime.fromisoformat(p_run_time_str.replace('Z', '+00:00'))
            else:
                p_ts = datetime.now(timezone.utc)
        except Exception:
            p_ts = datetime.now(timezone.utc)

        # 1. First attempt to heal from GCS validation_output.csv if run folder exists
        val_diag = None
        if p_run_folder:
            try:
                val_diag = await asyncio.to_thread(fetch_validation_failures, p_run_folder)
            except Exception as ve:
                log.warning(f"Sync: GCS validation check failed for {p_title}: {ve}")

        # 2. If no GCS failure found, attempt deep Cloud Logging query with 36h lookahead
        if not val_diag:
            try:
                val_diag = await diagnose_job_logs_async(p_job_id, run_time=p_ts)
            except Exception as le:
                log.warning(f"Sync: Deep log query failed for {p_title}: {le}")

        # 3. Update database if a better diagnosis was found
        if val_diag and val_diag.get('classification') not in ['UNKNOWN', 'PENDING_INDEXING']:
            try:
                async with db.write_lock:
                    if hasattr(db, 'db'):
                        db.db.collection(db.collection_name).document(p_job_id).set({
                            'log_diagnosis': val_diag,
                            'error_message': val_diag.get('error_message', p_error_msg)
                        }, merge=True)
                log.info(f"Sync: Successfully self-healed diagnosis for '{p_title}' ({p_job_id}) -> {val_diag.get('classification')}")
                discovered_count += 1
            except Exception as de:
                log.error(f"Sync: Failed to persist healed diagnosis for {p_job_id}: {de}")
                errors_count += 1
        elif p_job.get('status') == 'PENDING':
            event = FailureEvent(
                job_id=p_job_id,
                job_uid=p_job_id,
                import_name=p_title,
                stage_name="VALIDATION",
                status="FAILURE",
                message=p_error_msg,
                timestamp=p_ts,
                gcs_path=p_run_folder,
                job_name=p_job_name,
                region=p_job.get('region'),
                project_id=p_job.get('project_id')
            )
            try:
                log.info(f"Sync: Self-healing triaging pending job '{p_title}' (job_id={p_job_id})...")
                await run_stages(event, update_watermark=False)
                discovered_count += 1
            except Exception as pe:
                log.error(f"Sync: Self-healing triage failed for '{p_title}': {pe}")
                errors_count += 1

    if heal_candidates:
        log.info(f"Sync: Discovered {len(heal_candidates)} jobs in database needing self-healing triage. Processing...")
        batch_size = 5
        for i in range(0, len(heal_candidates), batch_size):
            batch = heal_candidates[i:i+batch_size]
            heal_tasks = [process_heal_job(p) for p in batch]
            await asyncio.gather(*heal_tasks)
            cleanup_memory()
            
    log.info(f"Background logs sync completed. Discovered {discovered_count} failures, encountered {errors_count} errors.")
    now_str = datetime.now(timezone.utc).isoformat()
    
    try:
        async with db.write_lock:
            db.update_last_sync_time(now_str)
    except Exception as e:
        log.error(f"Failed to save last sync time in database: {e}")
            
    await sse_manager.broadcast({
        "type": "sync_complete",
        "discovered_count": discovered_count,

        "errors_count": errors_count,
        "last_sync_time": now_str
    })

@app.post("/api/admin/sync-failures")
async def sync_failures(background_tasks: BackgroundTasks):
    if sync_lock.locked():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Log synchronization is already running."
        )
    background_tasks.add_task(run_sync_worker)
    return {"success": True, "message": "Log synchronization started in the background."}

@app.get("/api/admin/last-sync-time")
def get_last_sync_time():
    try:
        from .database import db
        t = db.get_last_sync_time()
        return {"last_sync_time": t}
    except Exception as e:
        log.error(f"Error fetching last sync time: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/admin/sync-status")
async def get_sync_status():
    return {
        "active": sync_lock.locked()
    }

@app.get("/api/csv-regression-diff/{issue_id}")
def get_csv_regression_diff(issue_id: str):
    try:
        diff_data = compute_csv_regression_diff(issue_id)
        if not diff_data:
            return {
                "previous_version": "",
                "current_version": "",
                "schema_diff": {"added_columns": [], "removed_columns": []},
                "variable_row_diff": []
            }
        return diff_data
    except Exception as e:
        log.error(f"Error computing CSV regression diff for issue {issue_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/pubsub", status_code=status.HTTP_202_ACCEPTED)
async def handle_pubsub_push(request: PubsubPushRequest, background_tasks: BackgroundTasks):
    try:
        log_entry = parse_pubsub_push(request)
        event = to_failure_event(log_entry)
        log.info(f"Accepted trigger event for {event.import_name} (stage={event.stage_name})")
        background_tasks.add_task(run_stages, event, update_watermark=False)
        return {"status": "accepted"}
    except NotAFailureEvent as e:
        log.info(f"Skipping non-trigger event: {e}")
        return {"status": "skipped"}
    except Exception as e:
        log.error(f"Error processing pubsub push payload: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Bad request payload: {e}"
        )
