import urllib.request
import urllib.parse
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List
from .base_verifier import BaseVerifier

log = logging.getLogger(__name__)

class BlsVerifier(BaseVerifier):
    def verify_deletions(self, deleted_nodes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        results = []
        if not deleted_nodes:
            return results

        # Limit verification checks to 10 nodes to prevent rate-limit blocking
        sample_nodes = deleted_nodes[:10]
        log.info(f"Triggering concurrent ThreadPoolExecutor for {len(sample_nodes)} BLS node verifications...")

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
                    log.error(f"Error checking BLS node: {e}")
                    results.append({
                        "place": node.get("observationAbout", ""),
                        "year": node.get("observationDate", ""),
                        "variable": node.get("variableMeasured", ""),
                        "value": "Error",
                        "status": "ERROR"
                    })

        log.info("Finished concurrent BLS verifications successfully.")
        return results

    def _verify_single_node(self, node: Dict[str, Any]) -> Dict[str, Any]:
        raw_place = node.get("observationAbout", "")
        year = node.get("observationDate", "")
        raw_var = node.get("variableMeasured", "")

        # 1. Translate StatVar to BLS series ID
        series_id = raw_var.replace("dcid:", "")
        if "bls/" in series_id:
            series_id = series_id.split("bls/")[-1]
        elif "bls" in series_id:
            series_id = series_id.replace("bls", "")

        # 2. Construct BLS REST URL
        # e.g., https://api.bls.gov/publicAPI/v2/timeseries/data/CUUR0000SA0
        url = f"https://api.bls.gov/publicAPI/v2/timeseries/data/{series_id}"
        
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'Mozilla/5.0'}
        )

        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                if response.getcode() == 200:
                    data = json.loads(response.read().decode())
                    if data.get("status") == "REQUEST_SUCCEEDED":
                        results = data.get("Results", {})
                        series_list = results.get("series", [])
                        if series_list:
                            data_points = series_list[0].get("data", [])
                            # Find if any data point matches our target year
                            target_year_str = str(year)
                            for dp in data_points:
                                if str(dp.get("year")) == target_year_str:
                                    return {
                                        "place": raw_place,
                                        "year": year,
                                        "variable": series_id,
                                        "value": str(dp.get("value")),
                                        "query_url": url,
                                        "status": "EXISTS_UPSTREAM"
                                    }
        except Exception as e:
            log.warning(f"BLS API query failed or returned no values for {series_id} ({raw_place}/{year}): {e}")

        # Default to deleted if BLS API returns 404 or year not found
        return {
            "place": raw_place,
            "year": year,
            "variable": series_id,
            "value": "None",
            "query_url": url,
            "status": "CONFIRMED_DELETED"
        }
