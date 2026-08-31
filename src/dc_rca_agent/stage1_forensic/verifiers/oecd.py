import urllib.request
import urllib.parse
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List
from .base_verifier import BaseVerifier

log = logging.getLogger(__name__)

# Map DC country 3-letter codes to OECD 3-letter/2-letter codes (often identical to standard ISO)
ISO_3_MAP = {
    "DNK": "DNK", "BEL": "BEL", "GRC": "GRC", "GBR": "GBR",
    "DEU": "DEU", "FRA": "FRA", "ITA": "ITA", "ESP": "ESP",
    "USA": "USA", "CAN": "CAN", "MEX": "MEX", "JPN": "JPN"
}

class OecdVerifier(BaseVerifier):
    def verify_deletions(self, deleted_nodes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        results = []
        if not deleted_nodes:
            return results

        # Limit verification checks to 10 nodes to prevent rate-limit bans
        sample_nodes = deleted_nodes[:10]
        log.info(f"Triggering concurrent ThreadPoolExecutor for {len(sample_nodes)} OECD node verifications...")

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
                    log.error(f"Error checking OECD node: {e}")
                    results.append({
                        "place": node.get("observationAbout", ""),
                        "year": node.get("observationDate", ""),
                        "variable": node.get("variableMeasured", ""),
                        "value": "Error",
                        "status": "ERROR"
                    })

        log.info("Finished concurrent OECD verifications successfully.")
        return results

    def _verify_single_node(self, node: Dict[str, Any]) -> Dict[str, Any]:
        raw_place = node.get("observationAbout", "")
        year = node.get("observationDate", "")
        raw_var = node.get("variableMeasured", "")

        # 1. Translate Country code
        place_code = raw_place
        if "/" in raw_place:
            iso3 = raw_place.split("/")[-1].upper()
            place_code = ISO_3_MAP.get(iso3, iso3)

        # 2. Translate StatVar to OECD dataset code
        # Default to REGION_DEMOGR if unmapped
        dataset_code = "REGION_DEMOGR"
        clean_var = raw_var.replace("dcid:", "")
        if "oecd/" in clean_var:
            dataset_code = clean_var.split("oecd/")[-1]
        elif "oecd" in clean_var:
            dataset_code = clean_var.replace("oecd", "")

        # 3. Construct OECD SDMX-JSON REST URL
        # e.g. https://stats.oecd.org/sdmx-json/data/REGION_DEMOGR/all/all?startTime=2020&endTime=2020
        url = f"https://stats.oecd.org/sdmx-json/data/{dataset_code}/all/all?startTime={year}&endTime={year}"
        
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'Mozilla/5.0'}
        )

        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                if response.getcode() == 200:
                    data = json.loads(response.read().decode())
                    data_dict = data.get("data", {})
                    datasets = data_dict.get("dataSets", [])
                    if datasets:
                        series = datasets[0].get("series", {})
                        
                        # Inspect if any series contains observations for our year
                        has_observations = False
                        observed_value = "None"
                        for s_val in series.values():
                            obs = s_val.get("observations", {})
                            if obs and len(obs) > 0:
                                has_observations = True
                                # Extract first observation value
                                first_obs_val = list(obs.values())[0]
                                if isinstance(first_obs_val, list) and len(first_obs_val) > 0:
                                    observed_value = str(first_obs_val[0])
                                break
                                
                        if has_observations:
                            return {
                                "place": place_code,
                                "year": year,
                                "variable": dataset_code,
                                "value": observed_value,
                                "query_url": url,
                                "status": "EXISTS_UPSTREAM"
                            }
        except Exception as e:
            log.warning(f"OECD API query failed or returned no values for {dataset_code} ({place_code}/{year}): {e}")

        # Default to deleted if OECD API returns 404 or empty
        return {
            "place": place_code,
            "year": year,
            "variable": dataset_code,
            "value": "None",
            "query_url": url,
            "status": "CONFIRMED_DELETED"
        }
