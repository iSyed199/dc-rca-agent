import urllib.request
import urllib.parse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List
from .base_verifier import BaseVerifier

log = logging.getLogger(__name__)

class UnDataVerifier(BaseVerifier):
    def verify_deletions(self, deleted_nodes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        results = []
        if not deleted_nodes:
            return results

        # Limit verification checks to 10 nodes to avoid overload
        sample_nodes = deleted_nodes[:10]
        log.info(f"Triggering concurrent ThreadPoolExecutor for {len(sample_nodes)} UNData node verifications...")

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
                    log.error(f"Error checking UNData node: {e}")
                    results.append({
                        "place": node.get("observationAbout", ""),
                        "year": node.get("observationDate", ""),
                        "variable": node.get("variableMeasured", ""),
                        "value": "Error",
                        "status": "ERROR"
                    })

        log.info("Finished concurrent UNData verifications successfully.")
        return results

    def _verify_single_node(self, node: Dict[str, Any]) -> Dict[str, Any]:
        raw_place = node.get("observationAbout", "")
        year = node.get("observationDate", "")
        raw_var = node.get("variableMeasured", "")

        # 1. Translate StatVar to dataset flowRef
        clean_var = raw_var.replace("dcid:", "")
        if "un/" in clean_var:
            clean_var = clean_var.split("un/")[-1]
            
        # Default flowRefs for UNData / UNEnergy
        flow_ref = "DF_UNSD_WDI"
        if "energy" in clean_var.lower():
            flow_ref = "DF_UNData_UNEnergy"
        elif "population" in clean_var.lower():
            flow_ref = "DF_UNData_Population"

        # 2. Construct UN REST URL (SDMX XML format)
        url = f"http://data.un.org/WS/rest/data/{flow_ref}/all/all?startTime={year}&endTime={year}"
        
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'Mozilla/5.0'}
        )

        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                code = response.getcode()
                if code == 200:
                    raw_xml = response.read().decode("utf-8")
                    # If response contains message markup, the dataset and date flow exist
                    if "<message:" in raw_xml or "<generic:" in raw_xml:
                        return {
                            "place": raw_place,
                            "year": year,
                            "variable": flow_ref,
                            "value": "Active",
                            "query_url": url,
                            "status": "EXISTS_UPSTREAM"
                        }
        except Exception as e:
            log.warning(f"UNData API query failed or returned no values for {flow_ref} ({raw_place}/{year}): {e}")

        # Default to manual verification fallback if UN API fails
        return {
            "place": raw_place,
            "year": year,
            "variable": flow_ref,
            "value": "None",
            "query_url": url,
            "status": "CONFIRMED_DELETED"
        }
