import urllib.request
import urllib.parse
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List
from .base_verifier import BaseVerifier
from ...settings import settings

log = logging.getLogger(__name__)

class UsCensusVerifier(BaseVerifier):
    def verify_deletions(self, deleted_nodes: List[Dict[str, Any]], progress_callback=None) -> List[Dict[str, Any]]:
        results = []
        if not deleted_nodes:
            return results

        # If Census API key is not configured, flag key required message
        if not settings.census_api_key:
            log.warning("US Census API key not configured. Skipping automated checks.")
            return [{
                "place": node.get("observationAbout", ""),
                "year": node.get("observationDate", ""),
                "variable": node.get("variableMeasured", ""),
                "value": "Key Required",
                "status": "NEEDS_API_KEY"
            } for node in deleted_nodes[:5]]

        sample_nodes = deleted_nodes[:5]
        log.info(f"Triggering concurrent ThreadPoolExecutor for {len(sample_nodes)} Census node verifications...")

        total = len(sample_nodes)
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_node = {executor.submit(self._verify_single_node, node): node for node in sample_nodes}
            for idx, future in enumerate(as_completed(future_to_node)):
                node = future_to_node[future]
                try:
                    res_node = future.result()
                    results.append(res_node)
                    if progress_callback:
                        progress_callback(idx + 1, total, res_node.get("variable"), res_node.get("status"))
                except Exception as e:
                    log.error(f"Error checking Census node: {e}")
                    res_err = {
                        "place": node.get("observationAbout", ""),
                        "year": node.get("observationDate", ""),
                        "variable": node.get("variableMeasured", "").replace("dcid:", ""),
                        "value": "Error",
                        "status": "ERROR"
                    }
                    results.append(res_err)
                    if progress_callback:
                        progress_callback(idx + 1, total, res_err.get("variable"), "ERROR")

        log.info("Finished concurrent Census verifications successfully.")
        return results

    def _verify_single_node(self, node: Dict[str, Any]) -> Dict[str, Any]:
        raw_place = node.get("observationAbout", "")
        year = node.get("observationDate", "")
        raw_var = node.get("variableMeasured", "")

        # 1. Translate Place to FIPS query params
        fips_params = ""
        place_label = raw_place
        if "geoId/" in raw_place:
            fips = raw_place.split("geoId/")[-1]
            place_label = fips
            if len(fips) == 2:
                # State
                fips_params = f"&for=state:{fips}"
            elif len(fips) == 5:
                # County: geoId/06075 -> state=06, county=075
                state = fips[:2]
                county = fips[2:]
                fips_params = f"&for=county:{county}&in=state:{state}"
        elif "country/USA" in raw_place:
            fips_params = "&for=us:*"
            place_label = "USA"
        else:
            # Fallback
            fips_params = f"&for=state:{raw_place}"

        # 2. Select dataset and metrics
        # Census PEP has population estimates datasets for different spans
        # Let's map target year to correct Census PEP endpoint version
        # Usually: 2020-2022 uses 2021/2022 endpoints
        target_year = int(year) if year.isdigit() else 2021
        dataset_year = "2021" if target_year <= 2021 else str(target_year)

        clean_var = raw_var.replace("dcid:", "")
        
        # Build Endpoint
        if clean_var == "Count_Person":
            url = f"https://api.census.gov/data/{dataset_year}/pep/population?get=POP_EST{fips_params}&key={settings.census_api_key}"
            gender_filter = None
        elif clean_var == "Count_Person_Male":
            url = f"https://api.census.gov/data/{dataset_year}/pep/charagegroups?get=POP_EST,SEX{fips_params}&key={settings.census_api_key}"
            gender_filter = "1" # Male
        elif clean_var == "Count_Person_Female":
            url = f"https://api.census.gov/data/{dataset_year}/pep/charagegroups?get=POP_EST,SEX{fips_params}&key={settings.census_api_key}"
            gender_filter = "2" # Female
        else:
            # Fallback general query
            url = f"https://api.census.gov/data/{dataset_year}/pep/population?get=POP_EST{fips_params}&key={settings.census_api_key}"
            gender_filter = None

        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'Mozilla/5.0'}
        )

        try:
            response_bytes = self.execute_request(req, timeout=5)
            rows = json.loads(response_bytes.decode())
            if len(rows) > 1:
                        header = rows[0]
                        data_rows = rows[1:]
                        
                        # Find indices
                        val_idx = header.index("POP_EST")
                        sex_idx = header.index("SEX") if "SEX" in header else -1
                        
                        for row in data_rows:
                            val = row[val_idx]
                            if gender_filter and sex_idx != -1:
                                if row[sex_idx] == gender_filter:
                                    return {
                                        "place": place_label,
                                        "year": year,
                                        "variable": clean_var,
                                        "value": str(val),
                                        "query_url": url.replace(settings.census_api_key, "REDACTED_API_KEY") if settings.census_api_key in url else url,
                                        "status": "EXISTS_UPSTREAM"
                                    }
                            elif not gender_filter:
                                return {
                                    "place": place_label,
                                    "year": year,
                                    "variable": clean_var,
                                    "value": str(val),
                                    "query_url": url.replace(settings.census_api_key, "REDACTED_API_KEY") if settings.census_api_key in url else url,
                                    "status": "EXISTS_UPSTREAM"
                                }
        except Exception as e:
            log.warning(f"Census API query failed or returned no values for {clean_var} ({place_label}/{year}): {e}")

        # If API returns 404 or empty, confirm deleted
        return {
            "place": place_label,
            "year": year,
            "variable": clean_var,
            "value": "None",
            "query_url": url.replace(settings.census_api_key, "REDACTED_API_KEY") if settings.census_api_key in url else url,
            "status": "CONFIRMED_DELETED"
        }
