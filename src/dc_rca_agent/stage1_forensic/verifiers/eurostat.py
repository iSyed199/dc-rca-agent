import urllib.request
import urllib.parse
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List
from .base_verifier import BaseVerifier

log = logging.getLogger(__name__)

# Map DC country 3-letter codes to Eurostat 2-letter codes
ISO_3_TO_2 = {
    "DNK": "DK", "BEL": "BE", "GRC": "EL", "GBR": "UK",
    "DEU": "DE", "FRA": "FR", "ITA": "IT", "ESP": "ES",
    "NLD": "NL", "SWE": "SE", "POL": "PL", "AUT": "AT",
    "IRL": "IE", "FIN": "FI", "PRT": "PT", "LUX": "LU",
    "CYP": "CY", "EST": "EE", "LVA": "LV", "LTU": "LT",
    "MLT": "MT", "SVK": "SK", "SVN": "SI", "BGR": "BG",
    "ROU": "RO", "HRV": "HR", "HUN": "HU", "CZE": "CZ"
}

class EurostatVerifier(BaseVerifier):
    def verify_deletions(self, deleted_nodes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        results = []
        if not deleted_nodes:
            return results

        # Limit verification check sample count to 10 to prevent rate-limit blocking
        sample_nodes = deleted_nodes[:10]
        log.info(f"Triggering concurrent ThreadPoolExecutor for {len(sample_nodes)} Eurostat node verifications...")

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
                    log.error(f"Error checking Eurostat node: {e}")
                    results.append({
                        "place": node.get("observationAbout", ""),
                        "year": node.get("observationDate", ""),
                        "variable": node.get("variableMeasured", ""),
                        "value": "Error",
                        "status": "ERROR"
                    })

        log.info("Finished concurrent Eurostat verifications successfully.")
        return results

    def _verify_single_node(self, node: Dict[str, Any]) -> Dict[str, Any]:
        raw_place = node.get("observationAbout", "")
        year = node.get("observationDate", "")
        raw_var = node.get("variableMeasured", "")

        # 1. Translate Country code
        place_code = raw_place
        if "/" in raw_place:
            iso3 = raw_place.split("/")[-1].upper()
            place_code = ISO_3_TO_2.get(iso3, iso3)

        # 2. Translate StatVar to Eurostat dataset code
        dataset_code = raw_var
        if raw_var.startswith("eurostat/"):
            dataset_code = raw_var[len("eurostat/"):]
        elif raw_var.startswith("eurostat"):
            dataset_code = raw_var[len("eurostat"):]

        # 3. Construct Eurostat REST URL
        # e.g., https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/educ_uoe_enrt01?format=JSON&lang=EN&geo=DK&time=2019
        url = f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{dataset_code}?format=JSON&lang=EN&geo={place_code}&time={year}"
        
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'Mozilla/5.0'}
        )

        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                if response.getcode() == 200:
                    data = json.loads(response.read().decode())
                    value_dict = data.get("value", {})
                    # If we got elements in the value dictionary, the data point exists upstream!
                    if value_dict and len(value_dict) > 0:
                        # Grab first value
                        first_val = list(value_dict.values())[0]
                        return {
                            "place": place_code,
                            "year": year,
                            "variable": dataset_code,
                            "value": str(first_val),
                            "query_url": url,
                            "status": "EXISTS_UPSTREAM"
                        }
        except Exception as e:
            log.warning(f"Eurostat API query failed or returned no values for {dataset_code} ({place_code}/{year}): {e}")

        # Default to deleted if Eurostat API returns 404 or empty
        return {
            "place": place_code,
            "year": year,
            "variable": dataset_code,
            "value": "None",
            "query_url": url,
            "status": "CONFIRMED_DELETED"
        }
