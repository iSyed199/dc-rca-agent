from typing import Dict, Any, List
from .base_verifier import BaseVerifier

class FallbackVerifier(BaseVerifier):
    def verify_deletions(self, deleted_nodes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        verification_results = []
        for node in deleted_nodes:
            verification_results.append({
                "node_id": node.get("Node", "unknown"),
                "statvar": node.get("variableMeasured", "").replace("dcid:", ""),
                "place": node.get("observationAbout", "").replace("dcid:", ""),
                "year": node.get("observationDate", ""),
                "status": "MANUAL_CHECK_REQUIRED",
                "value": None,
                "message": "Fallback: Manual verification required. Please click the provenance URL above to audit."
            })
        return verification_results
