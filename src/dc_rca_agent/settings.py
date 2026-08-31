from pydantic_settings import BaseSettings, SettingsConfigDict
import os

# Compute dynamic project root path relative to this file with environment override support
_root = os.environ.get("PROJECT_ROOT", os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="RCA_", env_file=".env", extra="ignore")

    project_id: str
    imports_bucket: str
    sync_window_days: int = 14
    pubsub_subscription_id: str = "ingestion-failures-pull-sub"
    
    # Resolve paths dynamically relative to project root
    imports_config_path: str = os.path.join(_root, "config", "imports.yaml")
    verification_results_path: str = os.path.join(_root, "config", "verification_results.json")
    verification_metadata_path: str = os.path.join(_root, "config", "verification_metadata.json")
    historical_archives_path: str = os.path.join(_root, "config", "historical_archives")
    
    # Default to system PATH lookup for gcloud; allows env override
    gcloud_bin_path: str = "gcloud"
    
    census_api_key: str | None = None
    eia_api_key: str | None = None
    fred_api_key: str | None = None

settings = Settings()

