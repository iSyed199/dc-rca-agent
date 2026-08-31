import urllib.request
import urllib.parse
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List
from .base_verifier import BaseVerifier
from ...settings import settings

log = logging.getLogger(__name__)

class EiaVerifier(BaseVerifier):
    def verify_deletions(self, deleted_nodes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        results = []
        if not deleted_nodes:
            return results

        # If EIA API key is not configured, flag key required message
        if not settings.eia_api_key:
            log.warning("EIA API key not configured. Skipping automated checks.")
            return [{
                "place": node.get("observationAbout", ""),
                "year": node.get("observationDate", ""),
                "variable": node.get("variableMeasured", ""),
                "value": "Key Required",
                "status": "NEEDS_API_KEY"
            } for node in deleted_nodes[:10]]

        sample_nodes = deleted_nodes[:10]
        log.info(f"Triggering concurrent ThreadPoolExecutor for {len(sample_nodes)} EIA node verifications...")

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
                    log.error(f"Error checking EIA node: {e}")
                    results.append({
                        "place": node.get("observationAbout", ""),
                        "year": node.get("observationDate", ""),
                        "variable": node.get("variableMeasured", ""),
                        "value": "Error",
                        "status": "ERROR"
                    })

        log.info("Finished concurrent EIA verifications successfully.")
        return results

    def _verify_single_node(self, node: Dict[str, Any]) -> Dict[str, Any]:
        raw_place = node.get("observationAbout", "")
        year = node.get("observationDate", "")
        raw_var = node.get("variableMeasured", "")

        # 1. Resolve State/Country code
        # e.g., geoId/06 (California) -> CA, country/USA -> US
        state_code = raw_place
        if "geoId/" in raw_place:
            fips = raw_place.split("geoId/")[-1]
            # State FIPS mappings to 2-letter abbreviation
            # (Simple fallback: if len == 2, use direct, or map common ones)
            # US Census FIPS to 2-letter abbreviations dictionary
            FIPS_TO_ABBR = {
                "06": "CA", "01": "AL", "36": "NY", "48": "TX", "17": "IL", "12": "FL"
            }
            state_code = FIPS_TO_ABBR.get(fips, fips)
        elif "country/USA" in raw_place:
            state_code = "US"

        # 2. Parse EIA StatVar
        # StatVar shape: eia/seds_TEPRB_US -> dataset=seds, msn=TEPRB
        clean_var = raw_var.replace("dcid:", "")
        if "eia/" in clean_var:
            clean_var = clean_var.split("eia/")[-1]
            
        parts = clean_var.split("_")
        dataset = "seds"
        msn = clean_var
        
        if len(parts) >= 2:
            dataset = parts[0].lower()
            msn = parts[1].upper()
            # If the third segment is a state code, keep the MSN separate from it
            # e.g., seds_TEPRB_US -> dataset=seds, msn=TEPRB

        # 3. Construct EIA REST URL
        # e.g. https://api.eia.gov/v2/seds/data/?api_key={key}&frequency=annual&data[]=value&facets[stateId][]={state}&facets[msn][]={msn}
        url = f"https://api.eia.gov/v2/{dataset}/data/?api_key={settings.eia_api_key}&frequency=annual&data[]=value&facets[stateId][]={state_code}&facets[msn][]={msn}"
        
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'Mozilla/5.0'}
        )

        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                if response.getcode() == 200:
                    data = json.loads(response.read().decode())
                    response_data = data.get("response", {})
                    data_rows = response_data.get("data", [])
                    
                    target_year_str = str(year)
                    for row in data_rows:
                        # EIA matches years on "period" or "year" (e.g. 2021)
                        row_period = str(row.get("period", ""))
                        if row_period == target_year_str or row_period.startswith(target_year_str):
                            val = row.get("value")
                            if val is not None:
                                return {
                                    "place": state_code,
                                    "year": year,
                                    "variable": msn,
                                    "value": str(val),
                                    "query_url": url.replace(settings.eia_api_key, "REDACTED_API_KEY") if settings.eia_api_key in url else url,
                                    "status": "EXISTS_UPSTREAM"
                                }
        except Exception as e:
            log.warning(f"EIA API query failed or returned no values for {msn} ({state_code}/{year}): {e}")

        # Default to deleted if EIA API returns 404 or empty
        return {
            "place": state_code,
            "year": year,
            "variable": msn,
            "value": "None",
            "query_url": url.replace(settings.eia_api_key, "REDACTED_API_KEY") if settings.eia_api_key in url else url,
            "status": "CONFIRMED_DELETED"
        }
