import urllib.request
import urllib.error
import time
import logging
import socket
from typing import Dict, Any, List

log = logging.getLogger(__name__)

class BaseVerifier:
    def verify_deletions(self, deleted_nodes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Verify if the deleted nodes are indeed deleted at the source.
        Returns a list of verification status details per node.
        """
        raise NotImplementedError()

    def execute_request(self, req: urllib.request.Request, timeout: int = 10, retries: int = 3, backoff_factor: float = 0.5) -> bytes:
        """
        Executes an HTTP request with retry logic (exponential backoff) for transient errors.
        """
        for attempt in range(retries):
            try:
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    return response.read()
            except (urllib.error.HTTPError, urllib.error.URLError, socket.timeout, ConnectionResetError) as e:
                is_transient = True
                if isinstance(e, urllib.error.HTTPError):
                    # Do not retry client errors (4xx) except for Rate Limiting (429) or transient server errors (5xx)
                    if 400 <= e.code < 500 and e.code != 429:
                        is_transient = False

                if not is_transient or attempt == retries - 1:
                    log.error(f"HTTP request failed permanently on attempt {attempt + 1}: {e}")
                    raise e
                
                sleep_time = backoff_factor * (2 ** attempt)
                log.warning(f"Transient error: {e}. Retrying in {sleep_time:.2f}s (Attempt {attempt + 1}/{retries})...")
                time.sleep(sleep_time)
        raise RuntimeError("Request failed after max retries.")
