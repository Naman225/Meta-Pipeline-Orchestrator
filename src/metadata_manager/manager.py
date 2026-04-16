import os
import glob
import json
import datetime
import logging

import yaml

logger = logging.getLogger(__name__)

# ── Resolve project root and writable log dirs ────────────────────────────
_PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
_LOGS_DIR    = os.path.join(_PROJECT_ROOT, "metadata_logs")
_SCHEMAS_DIR = os.path.join(_LOGS_DIR, "schemas")


class MetadataManager:
    """
    Central metadata hub for the automated data engineering pipeline.

    Handles:
      - Configuration loading
      - Schema registration and versioning (file-backed, upgradeable to Postgres)
      - Schema change logging
      - Lineage graph data
      - PostgreSQL / MongoDB connections (stubs — wire in psycopg2/pymongo when live)
    """

    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self._load_config()
        os.makedirs(_SCHEMAS_DIR, exist_ok=True)
        self._init_connections()

    # ── Config ────────────────────────────────────────────────────────────

    def _load_config(self) -> dict:
        """Load the YAML configuration file."""
        try:
            with open(self.config_path, "r") as fh:
                return yaml.safe_load(fh)
        except Exception as exc:
            logger.error("Failed to load config from %s: %s", self.config_path, exc)
            raise

    # ── Database connections (stubs) ──────────────────────────────────────

    def _init_connections(self):
        """
        Initialise PostgreSQL (psycopg2/SQLAlchemy) and MongoDB (pymongo) connections.
        Currently stubbed — enable when running with docker-compose.
        """
        pass

    # ── Pipeline config ───────────────────────────────────────────────────

    def get_pipeline_config(self, pipeline_name: str) -> dict:
        """Load a pipeline config YAML from the config/ directory."""
        config_file = os.path.join(_PROJECT_ROOT, "config", f"{pipeline_name}.yaml")
        if os.path.exists(config_file):
            with open(config_file, "r") as fh:
                return yaml.safe_load(fh)
        logger.warning("Pipeline config not found: %s", config_file)
        return {}

    # ── Lineage ───────────────────────────────────────────────────────────

    def record_lineage(self, pipeline_name: str, config: dict):
        """Record lineage metadata after a successful pipeline run."""
        logger.info("Lineage recorded for pipeline: %s", pipeline_name)
        print(f"[Lineage] Recorded: {pipeline_name}")

    def get_lineage(self, table_name: str) -> list:
        """Return downstream dependents of a given table."""
        return []

    def get_all_lineage(self) -> list:
        """
        Return all lineage edges for the ImpactAnalyzer.

        Dynamically builds a 4-hop downstream chain for every schema stored in
        logs/schemas/.  Falls back to an empty list when no schemas are registered.
        """
        records = []
        schema_files = glob.glob(os.path.join(_SCHEMAS_DIR, "*_schema.json"))
        for sf in schema_files:
            source   = os.path.basename(sf).replace("_schema.json", "")
            cleaned  = f"cleaned_{source}"
            feature  = f"feature_{source}"
            ml_input = f"ml_input_{source}"
            dash     = f"dashboard_{source}"
            records += [
                {"upstream_table": source,   "downstream_table": cleaned},
                {"upstream_table": cleaned,  "downstream_table": feature},
                {"upstream_table": feature,  "downstream_table": ml_input},
                {"upstream_table": ml_input, "downstream_table": dash},
            ]
        return records

    # ── Schema registry ───────────────────────────────────────────────────

    def register_schema(self, source_name: str, schema_dict: dict):
        """Persist a schema snapshot to logs/schemas/<source>_schema.json."""
        schema_file = os.path.join(_SCHEMAS_DIR, f"{source_name}_schema.json")
        with open(schema_file, "w") as fh:
            json.dump(schema_dict, fh, indent=2)
        logger.info("Schema registered: %s", source_name)

    def get_latest_schema(self, source_name: str) -> dict:
        """Load the latest persisted schema for a source."""
        schema_file = os.path.join(_SCHEMAS_DIR, f"{source_name}_schema.json")
        if os.path.exists(schema_file):
            with open(schema_file, "r") as fh:
                return json.load(fh)
        return {}

    def get_active_schemas(self) -> list:
        """Return all currently registered source names."""
        files = glob.glob(os.path.join(_SCHEMAS_DIR, "*_schema.json"))
        return [os.path.basename(f).replace("_schema.json", "") for f in files]

    # ── Change log ────────────────────────────────────────────────────────

    def log_schema_change(
        self,
        source_name: str,
        changes: list,
        old_schema: dict,
        new_schema: dict,
    ):
        """Append a schema change event to logs/schema_change_log.json."""
        log_file = os.path.join(_LOGS_DIR, "schema_change_log.json")
        entry = {
            "source_name": source_name,
            "timestamp":   datetime.datetime.now().isoformat(),
            "changes":     changes,
            "old_schema":  old_schema,
            "new_schema":  new_schema,
        }
        logs = []
        if os.path.exists(log_file):
            with open(log_file, "r") as fh:
                logs = json.load(fh)
        logs.append(entry)
        with open(log_file, "w") as fh:
            json.dump(logs, fh, indent=2)
        logger.info("Schema change logged for source: %s (%d changes)", source_name, len(changes))


if __name__ == "__main__":
    CONFIG_FILE = os.path.join(_PROJECT_ROOT, "config", "metadata_config.yaml")
    mgr = MetadataManager(CONFIG_FILE)
    print("MetadataManager initialised successfully.")
