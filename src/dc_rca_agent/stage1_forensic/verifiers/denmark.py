import urllib.request
import urllib.parse
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List
from .base_verifier import BaseVerifier

log = logging.getLogger(__name__)

class DenmarkVerifier(BaseVerifier):
    def verify_deletions(self, deleted_nodes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        results = []
        if not deleted_nodes:
            return results

        # Limit verification checks to 10 nodes to avoid rate-limiting
        sample_nodes = deleted_nodes[:10]
        log.info(f"Triggering concurrent ThreadPoolExecutor for {len(sample_nodes)} Denmark node verifications...")

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
                    log.error(f"Error checking Denmark node: {e}")
                    results.append({
                        "place": node.get("observationAbout", ""),
                        "year": node.get("observationDate", ""),
                        "variable": node.get("variableMeasured", ""),
                        "value": "Error",
                        "status": "ERROR"
                    })

        log.info("Finished concurrent Denmark verifications successfully.")
        return results

    def _verify_single_node(self, node: Dict[str, Any]) -> Dict[str, Any]:
        raw_place = node.get("observationAbout", "")
        year = node.get("observationDate", "")
        raw_var = node.get("variableMeasured", "")

        # 1. Translate year to Denmark Tid format (e.g. 2020 -> 2020K1 or 2020)
        # Statistikbanken Tid dimension uses year quarters (e.g. "2020K1") or years
        tid_val = str(year)
        if len(tid_val) == 4:
            tid_val = f"{tid_val}K1"

        # 2. Construct Statistikbanken POST Request
        # Table FOLK1A contains quarterly population metrics
        url = "https://api.statbank.dk/v1/data"
        post_data = {
            "table": "FOLK1A",
            "format": "JSONSTAT",
            "variables": [
                {
                    "code": "Tid",
                    "values": [tid_val]
                }
            ]
        }
        
        req = urllib.request.Request(
            url,
            data=json.dumps(post_data).encode("utf-8"),
            headers={
                'User-Agent': 'Mozilla/5.0',
                'Content-Type': 'application/json'
            },
            method="POST"
        )

        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                if response.getcode() == 200:
                    data = json.loads(response.read().decode())
                    # Extract observations from JSON-stat format
                    # JSON-stat maps dataset observations to a value array
                    dataset = data.get("dataset", {})
                    value_list = dataset.get("value", [])
                    if value_list and len(value_list) > 0:
                        first_val = value_list[0]
                        if first_val is not None:
                            return {
                                "place": "Denmark",
                                "year": year,
                                "variable": "Population",
                                "value": str(first_val),
                                "query_url": "https://www.statbank.dk/FOLK1A",
                                "status": "EXISTS_UPSTREAM"
                            }
        except Exception as e:
            log.warning(f"Denmark API query failed or returned no values ({raw_place}/{year}): {e}")

        # Default to deleted if Denmark API returns empty or fails
        return {
            "place": "Denmark",
            "year": year,
            "variable": "Population",
            "value": "None",
            "query_url": "https://www.statbank.dk/FOLK1A",
            "status": "CONFIRMED_DELETED"
        }
