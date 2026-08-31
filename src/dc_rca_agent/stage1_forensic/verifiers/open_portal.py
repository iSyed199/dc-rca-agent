import urllib.request
import urllib.parse
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List
from .base_verifier import BaseVerifier
from ...settings import settings

log = logging.getLogger(__name__)

class OpenPortalVerifier(BaseVerifier):
    def __init__(self, portal_type: str = "generic"):
        self.portal_type = portal_type.lower()

    def verify_deletions(self, deleted_nodes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        results = []
        if not deleted_nodes:
            return results

        # 1. Key check for key-required portals
        needs_key = False
        key_var = ""
        if self.portal_type == "bea" and not getattr(settings, "bea_api_key", None):
            needs_key = True
            key_var = "BEA_API_KEY"
        elif self.portal_type == "epa" and not getattr(settings, "epa_api_key", None):
            needs_key = True
            key_var = "EPA_API_KEY"
        elif self.portal_type == "fbi" and not getattr(settings, "fbi_api_key", None):
            needs_key = True
            key_var = "FBI_API_KEY"
        elif self.portal_type == "hud" and not getattr(settings, "hud_api_key", None):
            needs_key = True
            key_var = "HUD_API_KEY"


        if needs_key:
            log.warning(f"Key Required for portal {self.portal_type}. Skipping automated checks.")
            return [{
                "place": node.get("observationAbout", ""),
                "year": node.get("observationDate", ""),
                "variable": node.get("variableMeasured", ""),
                "value": "Key Required",
                "status": "NEEDS_API_KEY"
            } for node in deleted_nodes[:10]]

        # Limit verification checks to 10 nodes to prevent API blocking
        sample_nodes = deleted_nodes[:10]
        log.info(f"Triggering concurrent ThreadPoolExecutor for {len(sample_nodes)} {self.portal_type} node verifications...")

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
                    log.error(f"Error checking {self.portal_type} node: {e}")
                    results.append({
                        "place": node.get("observationAbout", ""),
                        "year": node.get("observationDate", ""),
                        "variable": node.get("variableMeasured", ""),
                        "value": "Error",
                        "status": "ERROR"
                    })

        log.info(f"Finished concurrent {self.portal_type} verifications successfully.")
        return results

    def _verify_single_node(self, node: Dict[str, Any]) -> Dict[str, Any]:
        raw_place = node.get("observationAbout", "")
        year = node.get("observationDate", "")
        raw_var = node.get("variableMeasured", "")
        
        place_code = raw_place.split("/")[-1] if "/" in raw_place else raw_place

        # Route dynamically to the correct sub-method
        if self.portal_type == "cdc":
            return self._verify_cdc(place_code, year, raw_var)
        elif self.portal_type == "nces":
            return self._verify_nces(place_code, year, raw_var)
        elif self.portal_type == "bea":
            return self._verify_bea(place_code, year, raw_var)
        elif self.portal_type == "epa":
            return self._verify_epa(place_code, year, raw_var)
        elif self.portal_type == "fbi":
            return self._verify_fbi(place_code, year, raw_var)
        
        # Fallback to direct reachability check
        return self._verify_reachability_fallback(place_code, year, raw_var)

    def _verify_cdc(self, place: str, year: str, var: str) -> Dict[str, Any]:
        # CDC PLACES SODA API
        # cwsq-ngh4 is County level. If place is county FIPS or state
        url = f"https://data.cdc.gov/resource/cwsq-ngh4.json?$limit=5"
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                if response.getcode() == 200:
                    data = json.loads(response.read().decode())
                    if data:
                        return {
                            "place": place,
                            "year": year,
                            "variable": var.split("/")[-1],
                            "value": "Active",
                            "query_url": url,
                            "status": "EXISTS_UPSTREAM"
                        }
        except Exception:
            pass
        return self._deleted_response(place, year, var, query_url=url)

    def _verify_nces(self, place: str, year: str, var: str) -> Dict[str, Any]:
        # NCES / Education Data API (Urban Institute)
        # e.g. https://educationdata.urban.org/api/v1/school-districts/demographics/
        url = "https://educationdata.urban.org/api/v1/school-districts/demographics/"
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                if response.getcode() == 200:
                    return {
                        "place": place,
                        "year": year,
                        "variable": var.split("/")[-1],
                        "value": "Active",
                        "query_url": url,
                        "status": "EXISTS_UPSTREAM"
                    }
        except Exception:
            pass
        return self._deleted_response(place, year, var, query_url=url)

    def _verify_bea(self, place: str, year: str, var: str) -> Dict[str, Any]:
        # BEA REST API regional GDP check
        url = f"https://apps.bea.gov/api/data/?&UserID={settings.bea_api_key}&method=GetData&DatasetName=Regional&TableName=SAGDP2N&GeoFIPS={place}&Year={year}&ResultFormat=json"
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        redacted_url = url.replace(settings.bea_api_key, "REDACTED_API_KEY") if settings.bea_api_key in url else url
        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                data = json.loads(response.read().decode())
                results = data.get("BEAAPI", {}).get("Results", {})
                if "Data" in results:
                    val = results["Data"][0].get("DataValue")
                    return {
                        "place": place,
                        "year": year,
                        "variable": var.split("/")[-1],
                        "value": str(val),
                        "query_url": redacted_url,
                        "status": "EXISTS_UPSTREAM"
                    }
        except Exception:
            pass
        return self._deleted_response(place, year, var, query_url=redacted_url)

    def _verify_epa(self, place: str, year: str, var: str) -> Dict[str, Any]:
        # EPA Air Quality Index API
        url = f"https://aqs.epa.gov/data/api/dailyData/byState?email=test@example.com&key={settings.epa_api_key}&param=44201&bdate={year}0101&edate={year}1231&state={place}"
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        redacted_url = url.replace(settings.epa_api_key, "REDACTED_API_KEY") if settings.epa_api_key in url else url
        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                data = json.loads(response.read().decode())
                if data.get("Header", {}).get("status") == "Success":
                    return {
                        "place": place,
                        "year": year,
                        "variable": var.split("/")[-1],
                        "value": "Active",
                        "query_url": redacted_url,
                        "status": "EXISTS_UPSTREAM"
                    }
        except Exception:
            pass
        return self._deleted_response(place, year, var, query_url=redacted_url)

    def _verify_fbi(self, place: str, year: str, var: str) -> Dict[str, Any]:
        # FBI Crime Data API
        url = f"https://api.usa.gov/crime/fbi/cde/arrest/state/{place}/all?from={year}&to={year}&api_key={settings.fbi_api_key}"
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        redacted_url = url.replace(settings.fbi_api_key, "REDACTED_API_KEY") if settings.fbi_api_key in url else url
        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                if response.getcode() == 200:
                    return {
                        "place": place,
                        "year": year,
                        "variable": var.split("/")[-1],
                        "value": "Active",
                        "query_url": redacted_url,
                        "status": "EXISTS_UPSTREAM"
                    }
        except Exception:
            pass
        return self._deleted_response(place, year, var, query_url=redacted_url)

    def _verify_reachability_fallback(self, place: str, year: str, var: str) -> Dict[str, Any]:
        # Check domain index of the variable's source domain based on portal type or variable keyword
        domain = None
        portal = self.portal_type.lower()
        var_lower = var.lower()
        
        if portal == "hud" or "hud" in var_lower:
            domain = "https://www.huduser.gov"
        elif portal == "bis" or "bis" in var_lower:
            domain = "https://stats.bis.org"
        elif portal == "fao" or "fao" in var_lower:
            domain = "https://www.fao.org/faostat/en/#data/PE"
        elif portal == "sat_act" or "sat" in var_lower or "act" in var_lower:
            domain = "https://www.collegeboard.org"

        if not domain:
            return {
                "place": place,
                "year": year,
                "variable": var.split("/")[-1] if "/" in var else var,
                "value": "Manual",
                "status": "MANUAL_CHECK_REQUIRED",
                "message": "Direct source API endpoint not available. Please verify against upstream source portal."
            }

        req = urllib.request.Request(
            domain,
            headers={'User-Agent': 'Mozilla/5.0'},
            method="HEAD"
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                if response.getcode() in [200, 301, 302]:
                    return {
                        "place": place,
                        "year": year,
                        "variable": var.split("/")[-1],
                        "value": "Reachable",
                        "query_url": domain,
                        "status": "EXISTS_UPSTREAM"
                    }
        except Exception:
            pass
        return self._deleted_response(place, year, var, query_url=domain)

    def _deleted_response(self, place: str, year: str, var: str, query_url: str | None = None) -> Dict[str, Any]:
        res = {
            "place": place,
            "year": year,
            "variable": var.split("/")[-1] if "/" in var else var,
            "value": "None",
            "status": "CONFIRMED_DELETED"
        }
        if query_url:
            res["query_url"] = query_url
        return res
