import logging
from .base_verifier import BaseVerifier
from .world_bank import WorldBankVerifier
from .eurostat import EurostatVerifier
from .us_census import UsCensusVerifier
from .oecd import OecdVerifier
from .bls import BlsVerifier
from .eia import EiaVerifier
from .un import UnDataVerifier
from .who import WhoVerifier
from .fred import FredVerifier
from .denmark import DenmarkVerifier
from .fao import FaoVerifier
from .open_portal import OpenPortalVerifier
from .fallback import FallbackVerifier

log = logging.getLogger(__name__)

def get_verifier(import_name: str) -> BaseVerifier:
    """
    Resolve and return the appropriate verification adapter for the given import.
    """
    normalized = import_name.lower()
    if "worldbank" in normalized or "world_bank" in normalized or "worlddevelopmentindicators" in normalized:
        return WorldBankVerifier()
    elif "eurostat" in normalized:
        return EurostatVerifier()
    elif "census" in normalized or "genderincomeinequality" in normalized or "retailsales" in normalized:
        return UsCensusVerifier()
    elif "oecd" in normalized:
        return OecdVerifier()
    elif "bls" in normalized:
        return BlsVerifier()
    elif "eia" in normalized:
        return EiaVerifier()
    elif "un" in normalized:
        return UnDataVerifier()
    elif "who" in normalized:
        return WhoVerifier()
    elif "fed" in normalized or "fred" in normalized:
        log.info(f"Routing to FredVerifier for import: {import_name}")
        return FredVerifier()
    elif "denmark" in normalized:
        return DenmarkVerifier()
    elif "fao" in normalized:
        return FaoVerifier()
    elif "cdc" in normalized:
        return OpenPortalVerifier("cdc")
    elif "nces" in normalized:
        return OpenPortalVerifier("nces")
    elif "bea" in normalized or "statesgdp" in normalized or "quarterlygdp" in normalized:
        return OpenPortalVerifier("bea")
    elif "epa" in normalized:
        return OpenPortalVerifier("epa")
    elif "fbi" in normalized:
        return OpenPortalVerifier("fbi")
    elif "hud" in normalized:
        return OpenPortalVerifier("hud")
    elif "bis" in normalized:
        return OpenPortalVerifier("bis")
    elif "sat_act" in normalized:
        return OpenPortalVerifier("sat_act")
    return FallbackVerifier()

