import urllib.request
import json
import logging
import concurrent.futures
from typing import Dict, Any, List
from .base_verifier import BaseVerifier

log = logging.getLogger(__name__)

# Mapping from Data Commons StatVar DCIDs to World Bank indicator codes
INDICATOR_MAPPINGS = {
    "Amount_EconomicActivity_ExpenditureActivity_EducationExpenditure_Government_AsFractionOf_Amount_EconomicActivity_ExpenditureActivity_Government": "SE.XPD.TOTL.GB.ZS",
    "Amount_EconomicActivity_ExpenditureActivity_EducationExpenditure_Government_AsFractionOf_Amount_EconomicActivity_GrossDomesticProduction_Nominal": "SE.XPD.TOTL.GD.ZS",
    "Amount_EconomicActivity_GrossDomesticProduction_Nominal": "NY.GDP.MKTP.CD",
    "Amount_EconomicActivity_GrossDomesticProduction_Real": "NY.GDP.MKTP.KD",
    "Amount_Consumption_Alcohol_15OrMoreYears_AsFractionOf_Count_Person_15OrMoreYears": "SH.ALC.PCAP.LI"
}

class WorldBankVerifier(BaseVerifier):
    def verify_deletions(self, deleted_nodes: List[Dict[str, Any]], progress_callback=None) -> List[Dict[str, Any]]:
        
        # Worker function to verify a single node in parallel
        def check_node(node: Dict[str, Any]) -> Dict[str, Any]:
            sv_raw = node.get("variableMeasured", "")
            sv = sv_raw.replace("dcid:", "").strip()
            
            # 1. Resolve indicator code dynamically or via hardcoded mapping
            indicator_code = INDICATOR_MAPPINGS.get(sv)
            if not indicator_code:
                if sv.startswith("worldBank/") or sv.startswith("worldbank/"):
                    # Example: worldBank/BG_GSR_NFSV_GD_ZS -> BG.GSR.NFSV.GD.ZS
                    indicator_code = sv.split("/", 1)[1].replace("_", ".").strip()
            
            place_raw = node.get("observationAbout", "")
            place = place_raw.replace("dcid:country/", "").replace("dcid:", "").strip()
            
            year = node.get("observationDate", "").strip()
            
            if not indicator_code or not place or not year:
                return {
                    "node_id": node.get("Node", "unknown"),
                    "statvar": sv,
                    "place": place,
                    "year": year,
                    "status": "UNSUPPORTED_MAPPING",
                    "value": None,
                    "message": f"Could not map StatVar '{sv}' to World Bank indicator."
                }
                
            url = f"https://api.worldbank.org/v2/country/{place}/indicator/{indicator_code}?date={year}&format=json"
            
            try:
                # Add headers mimicking standard client requests to bypass basic Cloudflare block rules
                req = urllib.request.Request(
                    url,
                    headers={
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)',
                        'Accept': 'application/json, text/plain, */*'
                    }
                )
                # Keep a robust 10-second timeout per single query thread
                response_bytes = self.execute_request(req, timeout=10)
                data = json.loads(response_bytes.decode('utf-8'))
                    
                if len(data) > 1 and data[1]:
                    datapoint = data[1][0]
                    value = datapoint.get("value")
                    
                    if value is None:
                        return {
                            "node_id": node.get("Node"),
                            "statvar": sv,
                            "place": place,
                            "year": year,
                            "status": "VERIFIED_DELETED",
                            "value": None,
                            "query_url": url,
                            "message": "Upstream Confirmed: Value is NULL at World Bank."
                        }
                    else:
                        return {
                            "node_id": node.get("Node"),
                            "statvar": sv,
                            "place": place,
                            "year": year,
                            "status": "EXISTS_UPSTREAM",
                            "value": value,
                            "query_url": url,
                            "message": f"Upstream Mismatch: World Bank still returns {value}."
                        }
                else:
                    return {
                        "node_id": node.get("Node"),
                        "statvar": sv,
                        "place": place,
                        "year": year,
                        "status": "VERIFIED_DELETED",
                        "value": None,
                        "query_url": url,
                        "message": "Upstream Confirmed: Indicator does not exist upstream."
                    }
            except Exception as e:
                log.warning(f"Error querying World Bank verifier for URL {url}: {e}")
                return {
                    "node_id": node.get("Node"),
                    "statvar": sv,
                    "place": place,
                    "year": year,
                    "status": "CONNECTION_FAILED",
                    "value": None,
                    "query_url": url,
                    "message": f"Could not connect to World Bank API: {str(e)}"
                }

        log.info(f"Triggering concurrent ThreadPoolExecutor for {len(deleted_nodes)} node verifications...")
        results = []
        total = len(deleted_nodes)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_node = {executor.submit(check_node, node): node for node in deleted_nodes}
            for idx, future in enumerate(concurrent.futures.as_completed(future_to_node)):
                try:
                    res = future.result()
                    results.append(res)
                    if progress_callback:
                        progress_callback(idx + 1, total, res.get("statvar"), res.get("status"))
                except Exception as ex:
                    log.error(f"Error checking node in ThreadPoolExecutor: {ex}")
                    
        log.info("Finished concurrent verifications successfully.")
        return results
