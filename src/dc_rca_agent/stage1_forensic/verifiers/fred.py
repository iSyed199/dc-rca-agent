import urllib.request
import urllib.parse
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List
from .base_verifier import BaseVerifier
from ...settings import settings

log = logging.getLogger(__name__)

class FredVerifier(BaseVerifier):
    def verify_deletions(self, deleted_nodes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        results = []
        if not deleted_nodes:
            return results

        # If FRED API key is not configured, flag key required message
        if not settings.fred_api_key:
            log.warning("FRED API key not configured. Skipping automated checks.")
            return [{
                "place": node.get("observationAbout", ""),
                "year": node.get("observationDate", ""),
                "variable": node.get("variableMeasured", ""),
                "value": "Key Required",
                "status": "NEEDS_API_KEY"
            } for node in deleted_nodes[:10]]

        sample_nodes = deleted_nodes[:10]
        log.info(f"Triggering concurrent ThreadPoolExecutor for {len(sample_nodes)} FRED node verifications...")

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_node = {}
            for node in sample_nodes:
                future = executor.submit(self._verify_single_node, node)
                future_to_node[future] = node

            for future in as_completed(future_to_node):
                node = future_to_node[future]
                try:
                    res_node = future.result()
                    results.append(res_node)
                except Exception as e:
                    log.error(f"Error checking FRED node: {e}")
                    results.append({
                        "place": node.get("observationAbout", ""),
                        "year": node.get("observationDate", ""),
                        "variable": node.get("variableMeasured", ""),
                        "value": "Error",
                        "status": "ERROR"
                    })

        log.info("Finished concurrent FRED verifications successfully.")
        return results

    def _verify_single_node(self, node: Dict[str, Any]) -> Dict[str, Any]:
        raw_place = node.get("observationAbout", "")
        year = node.get("observationDate", "")
        raw_var = node.get("variableMeasured", "")

        # 1. Translate StatVar to FRED Series ID
        series_id = raw_var.replace("dcid:", "")
        if "fed/" in series_id:
            series_id = series_id.split("fed/")[-1]
        elif "fed" in series_id:
            series_id = series_id.replace("fed", "")

        # Default fallback series IDs for Treasury / interest rates
        if not series_id or len(series_id) < 2:
            series_id = "DGS10"  # default to 10-year constant maturity

        # 2. Construct FRED REST URL
        url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={settings.fred_api_key}&file_type=json"
        
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'Mozilla/5.0'}
        )

        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                if response.getcode() == 200:
                    data = json.loads(response.read().decode())
                    observations = data.get("observations", [])
                    
                    target_year_str = str(year)
                    for obs in observations:
                        obs_date = str(obs.get("date", ""))
                        # FRED dates shape: 2021-07-01 or 2021
                        if obs_date == target_year_str or obs_date.startswith(target_year_str):
                            val = obs.get("value")
                            if val is not None and val != ".":
                                return {
                                    "place": raw_place,
                                    "year": year,
                                    "variable": series_id,
                                    "value": str(val),
                                    "query_url": url.replace(settings.fred_api_key, "REDACTED_API_KEY") if settings.fred_api_key in url else url,
                                    "status": "EXISTS_UPSTREAM"
                                }
        except Exception as e:
            log.warning(f"FRED API query failed or returned no values for {series_id} ({raw_place}/{year}): {e}")

        # Default to deleted if FRED API returns 404 or empty matching observations
        return {
            "place": raw_place,
            "year": year,
            "variable": series_id,
            "value": "None",
            "query_url": url.replace(settings.fred_api_key, "REDACTED_API_KEY") if settings.fred_api_key in url else url,
            "status": "CONFIRMED_DELETED"
        }
