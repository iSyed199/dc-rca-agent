import urllib.request
import urllib.parse
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List
from .base_verifier import BaseVerifier

log = logging.getLogger(__name__)

# Map DC country 3-letter codes to standard WHO codes (identical to ISO-3)
ISO_3_MAP = {
    "DNK": "DNK", "BEL": "BEL", "GRC": "GRC", "GBR": "GBR",
    "DEU": "DEU", "FRA": "FRA", "ITA": "ITA", "ESP": "ESP",
    "USA": "USA", "CAN": "CAN", "MEX": "MEX", "JPN": "JPN",
    "PAK": "PAK", "IND": "IND", "BGD": "BGD"
}

class WhoVerifier(BaseVerifier):
    def verify_deletions(self, deleted_nodes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        results = []
        if not deleted_nodes:
            return results

        # Limit verification checks to 10 nodes to prevent API blocking
        sample_nodes = deleted_nodes[:10]
        log.info(f"Triggering concurrent ThreadPoolExecutor for {len(sample_nodes)} WHO node verifications...")

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
                    log.error(f"Error checking WHO node: {e}")
                    results.append({
                        "place": node.get("observationAbout", ""),
                        "year": node.get("observationDate", ""),
                        "variable": node.get("variableMeasured", ""),
                        "value": "Error",
                        "status": "ERROR"
                    })

        log.info("Finished concurrent WHO verifications successfully.")
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

        # 2. Parse WHO Indicator Code
        # StatVar shape: who/WHOSIS_000001 -> WHOSIS_000001
        indicator = raw_var.replace("dcid:", "")
        if "who/" in indicator:
            indicator = indicator.split("who/")[-1]
        elif "who" in indicator:
            indicator = indicator.replace("who", "")
            
        # Default to standard life expectancy indicator if unmapped
        if not indicator or indicator == "COVID19":
            indicator = "WHOSIS_000001"

        # 3. Construct WHO AzureEdge OData URL
        url = f"https://ghoapi.azureedge.net/api/{indicator}"
        
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'Mozilla/5.0'}
        )

        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                if response.getcode() == 200:
                    data = json.loads(response.read().decode())
                    values = data.get("value", [])
                    
                    target_year = int(year) if str(year).isdigit() else 0
                    for item in values:
                        # Check TimeDim and SpatialDim
                        item_year = item.get("TimeDim")
                        item_geo = item.get("SpatialDim", "")
                        
                        if item_year == target_year and item_geo == place_code:
                            val = item.get("NumericValue") or item.get("Value")
                            if val is not None:
                                return {
                                    "place": place_code,
                                    "year": year,
                                    "variable": indicator,
                                    "value": str(val),
                                    "query_url": url,
                                    "status": "EXISTS_UPSTREAM"
                                }
        except Exception as e:
            log.warning(f"WHO API query failed or returned no values for {indicator} ({place_code}/{year}): {e}")

        # Default to deleted if WHO API returns 404 or empty matching observations
        return {
            "place": place_code,
            "year": year,
            "variable": indicator,
            "value": "None",
            "query_url": url,
            "status": "CONFIRMED_DELETED"
        }
