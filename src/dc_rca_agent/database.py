import os
import json
import logging
from datetime import datetime
from typing import List, Dict, Any
from .models import FailureEvent
from .settings import settings

log = logging.getLogger(__name__)

# Try importing Firestore SDK (optional dependency for local run)
try:
    from google.cloud import firestore
except ImportError:
    firestore = None

class BaseDatabase:
    @property
    def write_lock(self):
        import asyncio
        if not hasattr(self, '_write_lock') or self._write_lock is None:
            self._write_lock = asyncio.Lock()
        return self._write_lock

    def get_all_results(self) -> List[Dict[str, Any]]:
        raise NotImplementedError()

    def save_or_update_result(
        self,
        event: FailureEvent,
        status: str,
        run_folder: str | None,
        diff_summary: Dict[str, Any] | None,
        provenance_url: str | None = None,
        provenance_description: str | None = None,
        has_golden_config: bool | None = None,
        log_diagnosis: Dict[str, Any] | None = None
    ) -> None:
        raise NotImplementedError()

    def resolve_failure_record(
        self,
        target_job_id: str,
        run_folder: str | None,
        diff_summary: Dict[str, Any] | None,
        provenance_url: str | None,
        provenance_description: str | None,
        successful_job_id: str,
        successful_job_name: str,
        successful_run_time: str,
        successful_region: str | None = None,
        successful_project_id: str | None = None,
        has_golden_config: bool | None = None
    ) -> None:
        raise NotImplementedError()

    def update_issue_id(self, job_id: str, issue_id: str) -> None:
        raise NotImplementedError()

    def update_rca_detail(self, job_id: str, rca_detail: str) -> None:
        raise NotImplementedError()

    def update_verification_results(self, job_id: str, verification_results: Dict[str, Any]) -> None:
        raise NotImplementedError()

    def seed_from_json(self, json_path: str) -> None:
        raise NotImplementedError()

    def get_last_sync_time(self) -> str | None:
        raise NotImplementedError()

    def update_last_sync_time(self, timestamp: str) -> None:
        raise NotImplementedError()

class LocalJsonDatabase(BaseDatabase):
    def __init__(self):
        self.path = settings.verification_results_path

    def _read_data(self) -> List[Dict[str, Any]]:
        if not os.path.exists(self.path):
            return []
        try:
            with open(self.path, 'r') as f:
                return json.load(f)
        except Exception as e:
            log.error(f"Error reading local JSON database: {e}")
            return []

    def _write_data(self, data: List[Dict[str, Any]]) -> None:
        try:
            with open(self.path, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            log.error(f"Error writing local JSON database: {e}")

    def get_all_results(self) -> List[Dict[str, Any]]:
        return self._read_data()

    def save_or_update_result(
        self,
        event: FailureEvent,
        status: str,
        run_folder: str | None,
        diff_summary: Dict[str, Any] | None,
        provenance_url: str | None = None,
        provenance_description: str | None = None,
        has_golden_config: bool | None = None,
        log_diagnosis: Dict[str, Any] | None = None
    ) -> None:
        import re
        
        def _extract_pct(msg: str) -> float | None:
            if not msg:
                return None
            m = re.search(r"Found\s+([\d.]+)\%\s+deleted\s+records", msg, re.IGNORECASE)
            if m:
                try:
                    return float(m.group(1))
                except ValueError:
                    pass
            return None

        # Calculate deletion percentage and class
        pct = None
        deletion_class = None
        if diff_summary:
            deleted = diff_summary.get('deleted_obs_count', 0)
            pct = _extract_pct(event.message)
            if pct is None:
                total = diff_summary.get('total_obs_count') or diff_summary.get('previous_obs_count') or 1000000
                pct = round((deleted / total) * 100, 4) if total > 0 else 0.0
            
            # Deletions greater than 0.1% are MAJOR; less than or equal to 0.1% are MINOR
            if pct > 0:
                deletion_class = "MAJOR" if pct > 0.1 else "MINOR"

        data = self._read_data()
        cleaned_data = []
        found = False
        
        for item in data:
            if item.get('title') == event.import_name:
                if not found:
                    if item.get('status') == 'RESOLVED' and item.get('run_time') and event.timestamp:
                        try:
                            current_ts = datetime.fromisoformat(item['run_time'].replace('Z', '+00:00'))
                            if event.timestamp <= current_ts:
                                log.info(f"Skipping save_or_update_result for '{event.import_name}': incoming event timestamp ({event.timestamp}) is not newer than active database run_time ({current_ts}).")
                                cleaned_data.append(item)
                                found = True
                                continue
                        except Exception as te:
                            log.warning(f"Error comparing timestamps: {te}")

                    item['job_id'] = event.job_id
                    item['status'] = status
                    item['error_message'] = event.message
                    if event.job_name:
                        item['job_name'] = event.job_name
                    if event.region:
                        item['region'] = event.region
                    if event.project_id:
                        item['project_id'] = event.project_id
                    if event.timestamp:
                        item['run_time'] = event.timestamp.isoformat()
                    item['latest_run_folder'] = run_folder
                    item['differ_summary'] = diff_summary
                    item['provenance_url'] = provenance_url
                    item['provenance_description'] = provenance_description
                    item['deletion_percentage'] = pct
                    item['deletion_class'] = deletion_class
                    if has_golden_config is not None:
                        item['has_golden_config'] = has_golden_config
                    if log_diagnosis is not None:
                        item['log_diagnosis'] = log_diagnosis
                    cleaned_data.append(item)
                    found = True
                else:
                    # Deduplicate and drop duplicate entries
                    continue
            else:
                cleaned_data.append(item)

        if not found:
            cleaned_data.append({
                "issue_num": str(len(cleaned_data) + 1),
                "issue_id": "",
                "job_id": event.job_id,
                "job_name": event.job_name,
                "region": event.region,
                "project_id": event.project_id,
                "title": event.import_name,
                "status": status,
                "run_time": event.timestamp.isoformat() if event.timestamp else None,
                "latest_run_folder": run_folder,
                "differ_summary": diff_summary,
                "provenance_url": provenance_url,
                "provenance_description": provenance_description,
                "error_message": event.message,
                "deletion_percentage": pct,
                "deletion_class": deletion_class,
                "has_golden_config": has_golden_config
            })

        # Re-index issue_num to maintain sequential order
        for idx, item in enumerate(cleaned_data):
            item['issue_num'] = str(idx + 1)

        self._write_data(cleaned_data)
        log.info(f"Successfully saved result locally to {self.path}")

    def resolve_failure_record(
        self,
        target_job_id: str,
        run_folder: str | None,
        diff_summary: Dict[str, Any] | None,
        provenance_url: str | None,
        provenance_description: str | None,
        successful_job_id: str,
        successful_job_name: str,
        successful_run_time: str,
        successful_region: str | None = None,
        successful_project_id: str | None = None,
        has_golden_config: bool | None = None
    ) -> None:
        data = self._read_data()
        
        # Resolve target title first
        target_title = None
        for item in data:
            if item.get('job_id') == target_job_id:
                target_title = item.get('title')
                break
                
        cleaned_data = []
        found = False
        
        for item in data:
            if target_title and item.get('title') == target_title:
                if not found:
                    item['status'] = 'RESOLVED'
                    item['run_time'] = successful_run_time
                    item['error_message'] = f"Import: {item.get('title')} stage: FINISH status: SUCCESS"
                    item['latest_run_folder'] = run_folder
                    item['differ_summary'] = diff_summary
                    item['provenance_url'] = provenance_url
                    item['provenance_description'] = provenance_description
                    item['has_golden_config'] = has_golden_config
                    item['job_id'] = successful_job_id
                    item['job_name'] = successful_job_name
                    if successful_region:
                        item['region'] = successful_region
                    if successful_project_id:
                        item['project_id'] = successful_project_id
                    cleaned_data.append(item)
                    found = True
                else:
                    # Deduplicate and drop duplicate entries
                    continue
            else:
                cleaned_data.append(item)

        # Re-index issue_num to maintain sequential order
        for idx, item in enumerate(cleaned_data):
            item['issue_num'] = str(idx + 1)

        self._write_data(cleaned_data)
        log.info(f"Successfully resolved failure record '{target_job_id}' locally.")

    def update_issue_id(self, job_id: str, issue_id: str) -> None:
        data = self._read_data()
        for item in data:
            if item.get('job_id') == job_id:
                item['issue_id'] = issue_id
                break
        self._write_data(data)
        log.info(f"Updated issue_id to {issue_id} for job {job_id} locally.")

    def update_rca_detail(self, job_id: str, rca_detail: str) -> None:
        data = self._read_data()
        for item in data:
            if item.get('job_id') == job_id:
                item['rca_detail'] = rca_detail
                break
        self._write_data(data)
        log.info(f"Updated rca_detail for job {job_id} locally.")

    def update_verification_results(self, job_id: str, verification_results: Dict[str, Any]) -> None:
        data = self._read_data()
        for item in data:
            if item.get('job_id') == job_id or item.get('issue_id') == job_id:
                item['verification_results'] = verification_results
                break
        self._write_data(data)
        log.info(f"Updated verification_results for job {job_id} locally.")

    def seed_from_json(self, json_path: str) -> None:
        log.info("LOCAL mode: Skipping database seeding.")

    def get_last_sync_time(self) -> str | None:
        metadata_path = self.path.replace("verification_results.json", "verification_metadata.json")
        if not os.path.exists(metadata_path):
            return None
        try:
            with open(metadata_path, 'r') as f:
                meta = json.load(f)
                return meta.get("last_sync_time")
        except Exception as e:
            log.error(f"Error reading local metadata: {e}")
            return None

    def update_last_sync_time(self, timestamp: str) -> None:
        metadata_path = self.path.replace("verification_results.json", "verification_metadata.json")
        try:
            meta = {}
            if os.path.exists(metadata_path):
                with open(metadata_path, 'r') as f:
                    meta = json.load(f)
            meta["last_sync_time"] = timestamp
            with open(metadata_path, 'w') as f:
                json.dump(meta, f, indent=2)
            log.info(f"Updated last_sync_time to {timestamp} locally.")
        except Exception as e:
            log.error(f"Error writing local metadata: {e}")

class FirestoreDatabase(BaseDatabase):
    def __init__(self):
        if not firestore:
            raise RuntimeError("google-cloud-firestore package is required for Firestore mode.")
        self.db = firestore.Client()
        self.collection_name = "failures"

    def get_all_results(self) -> List[Dict[str, Any]]:
        try:
            docs = self.db.collection(self.collection_name).stream()
            results = []
            for doc in docs:
                data = doc.to_dict()
                data["job_id"] = doc.id
                results.append(data)
            # Sort by issue_num numerically if present
            results.sort(key=lambda x: int(x.get('issue_num', 999)))
            return results
        except Exception as e:
            log.error(f"Error reading Firestore collection: {e}")
            return []

    def save_or_update_result(
        self,
        event: FailureEvent,
        status: str,
        run_folder: str | None,
        diff_summary: Dict[str, Any] | None,
        provenance_url: str | None = None,
        provenance_description: str | None = None,
        has_golden_config: bool | None = None,
        log_diagnosis: Dict[str, Any] | None = None
    ) -> None:
        try:
            doc_ref = self.db.collection(self.collection_name).document(event.job_id)
            doc = doc_ref.get()
            
            update_payload = {
                "title": event.import_name,
                "status": status,
                "job_id": event.job_id,
                "job_name": event.job_name,
                "region": event.region,
                "project_id": event.project_id,
                "run_time": event.timestamp.isoformat() if event.timestamp else None,
                "latest_run_folder": run_folder,
                "differ_summary": diff_summary,
                "provenance_url": provenance_url,
                "provenance_description": provenance_description,
                "error_message": event.message,
                "has_golden_config": has_golden_config,
                "log_diagnosis": log_diagnosis
            }
            
            if doc.exists:
                doc_data = doc.to_dict() or {}
                if doc_data.get('status') == 'RESOLVED' and doc_data.get('run_time') and event.timestamp:
                    try:
                        current_ts = datetime.fromisoformat(doc_data['run_time'].replace('Z', '+00:00'))
                        if event.timestamp <= current_ts:
                            log.info(f"Skipping save_or_update_result for '{event.import_name}': incoming event timestamp ({event.timestamp}) is not newer than active database run_time ({current_ts}).")
                            return
                    except Exception as te:
                        log.warning(f"Error comparing timestamps in Firestore write: {te}")

                doc_ref.update({k: v for k, v in update_payload.items() if v is not None})
            else:
                total_docs = len(list(self.db.collection(self.collection_name).list_documents()))
                
                new_doc = {
                    "issue_num": str(total_docs + 1),
                    "issue_id": "",
                    "job_id": event.job_id,
                    "job_name": event.job_name,
                    "region": event.region,
                    "project_id": event.project_id,
                    "title": event.import_name,
                    "status": status,
                    "run_time": event.timestamp.isoformat() if event.timestamp else None,
                    "latest_run_folder": run_folder,
                    "differ_summary": diff_summary,
                    "provenance_url": provenance_url,
                    "provenance_description": provenance_description,
                    "error_message": event.message,
                    "has_golden_config": has_golden_config,
                    "log_diagnosis": log_diagnosis
                }
                doc_ref.set(new_doc)
            log.info(f"Successfully saved result in Firestore collection '{self.collection_name}'")
        except Exception as e:
            log.error(f"Error writing to Firestore: {e}")

    def resolve_failure_record(
        self,
        target_job_id: str,
        run_folder: str | None,
        diff_summary: Dict[str, Any] | None,
        provenance_url: str | None,
        provenance_description: str | None,
        successful_job_id: str,
        successful_job_name: str,
        successful_run_time: str,
        successful_region: str | None = None,
        successful_project_id: str | None = None,
        has_golden_config: bool | None = None
    ) -> None:
        try:
            doc_ref = self.db.collection(self.collection_name).document(target_job_id)
            update_payload = {
                "status": "RESOLVED",
                "run_time": successful_run_time,
                "latest_run_folder": run_folder,
                "differ_summary": diff_summary,
                "provenance_url": provenance_url,
                "provenance_description": provenance_description,
                "has_golden_config": has_golden_config,
                "error_message": f"Import: resolved by SUCCESS job {successful_job_id}",
                "job_id": successful_job_id,
                "job_name": successful_job_name,
                "region": successful_region,
                "project_id": successful_project_id
            }
            doc_ref.update({k: v for k, v in update_payload.items() if v is not None})
            log.info(f"Successfully resolved failure record '{target_job_id}' in Firestore")
        except Exception as e:
            log.error(f"Error resolving failure record '{target_job_id}' in Firestore: {e}")

    def update_issue_id(self, job_id: str, issue_id: str) -> None:
        try:
            doc_ref = self.db.collection(self.collection_name).document(job_id)
            doc_ref.update({"issue_id": issue_id})
            log.info(f"Updated issue_id to {issue_id} for job {job_id} in Firestore.")
        except Exception as e:
            log.error(f"Error updating issue_id in Firestore: {e}")

    def update_rca_detail(self, job_id: str, rca_detail: str) -> None:
        try:
            doc_ref = self.db.collection(self.collection_name).document(job_id)
            doc_ref.update({"rca_detail": rca_detail})
            log.info(f"Updated rca_detail for job {job_id} in Firestore.")
        except Exception as e:
            log.error(f"Error updating rca_detail in Firestore: {e}")

    def update_verification_results(self, job_id: str, verification_results: Dict[str, Any] | None) -> None:
        try:
            if verification_results is None:
                doc_ref = self.db.collection(self.collection_name).document(job_id)
                if doc_ref.get().exists:
                    doc_ref.update({"verification_results": None})
                    log.info(f"Cleared verification_results for job {job_id} in Firestore.")
                    return
                docs = self.db.collection(self.collection_name).where("issue_id", "==", job_id).stream()
                for doc in docs:
                    doc.reference.update({"verification_results": None})
                return

            doc_ref = self.db.collection(self.collection_name).document(job_id)
            doc = doc_ref.get()
            if doc.exists:
                existing = doc.to_dict().get("verification_results") or {}
                merged = {**existing, **verification_results} if isinstance(existing, dict) else verification_results
                doc_ref.update({"verification_results": merged})
                log.info(f"Updated and merged verification_results for job {job_id} in Firestore.")
                return
            
            docs = self.db.collection(self.collection_name).where("issue_id", "==", job_id).stream()
            for doc in docs:
                existing = doc.to_dict().get("verification_results") or {}
                merged = {**existing, **verification_results} if isinstance(existing, dict) else verification_results
                doc.reference.update({"verification_results": merged})
                log.info(f"Updated and merged verification_results for issue_id {job_id} in Firestore.")
        except Exception as e:
            log.error(f"Error updating verification_results in Firestore: {e}")

    def seed_from_json(self, json_path: str, metadata_path: str | None = None) -> None:
        try:
            # Check if any documents already exist
            docs = list(self.db.collection(self.collection_name).limit(1).stream())
            if docs:
                log.info("Firestore collection already contains records. Skipping database seeding.")
                return
            
            log.info(f"Firestore collection is empty. Auto-seeding from {json_path}...")
            if not os.path.exists(json_path):
                log.warning(f"Seeding source file {json_path} not found.")
                return
                
            with open(json_path, 'r') as f:
                data = json.load(f)
            
            batch = self.db.batch()
            for index, item in enumerate(data):
                job_id = item.get("job_id")
                if not job_id:
                    continue
                doc_ref = self.db.collection(self.collection_name).document(job_id)
                batch.set(doc_ref, item)
                
                if (index + 1) % 400 == 0:
                    batch.commit()
                    batch = self.db.batch()
            batch.commit()
            log.info("Firestore auto-seeding completed successfully!")

            # Seed last_sync_time metadata if metadata_path is provided
            if metadata_path and os.path.exists(metadata_path):
                try:
                    with open(metadata_path, 'r') as f:
                        meta_data = json.load(f)
                    last_sync = meta_data.get("last_sync_time")
                    if last_sync:
                        self.update_last_sync_time(last_sync)
                except Exception as me:
                    log.error(f"Error seeding metadata from JSON: {me}")
                    
        except Exception as e:
            log.error(f"Error during Firestore database seeding: {e}")

    def get_last_sync_time(self) -> str | None:
        try:
            doc_ref = self.db.collection("metadata").document("sync")
            doc = doc_ref.get()
            if doc.exists:
                return doc.to_dict().get("last_sync_time")
        except Exception as e:
            log.error(f"Error reading last_sync_time from Firestore: {e}")
        return None

    def update_last_sync_time(self, timestamp: str) -> None:
        try:
            doc_ref = self.db.collection("metadata").document("sync")
            doc_ref.set({"last_sync_time": timestamp}, merge=True)
            log.info(f"Updated last_sync_time to {timestamp} in Firestore.")
        except Exception as e:
            log.error(f"Error updating last_sync_time in Firestore: {e}")

# Database Service Selector Factory
def get_db() -> BaseDatabase:
    db_type = os.environ.get("DATABASE_TYPE", "FIRESTORE").upper()
    if db_type == "FIRESTORE" and firestore is not None:
        log.info("RCA Agent database initialized in FIRESTORE mode.")
        return FirestoreDatabase()
    else:
        log.info("RCA Agent database initialized in LOCAL mode.")
        return LocalJsonDatabase()

db = get_db()

