import json
import os
import sys

# Add src to pythonpath so we can import manager
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "src")))

from metadata_manager.manager import MetadataManager
from schema_registry.registry import SchemaRegistryService

class MockField:
    def __init__(self, name, dataType):
        self.name = name
        self.dataType = dataType

class MockSchema:
    def __init__(self, fields):
        self.fields = fields

class MockDF:
    def __init__(self, fields):
        self.schema = MockSchema(fields)

def main():
    print("Initializing MetadataManager and SchemaRegistryService...")
    # Initialize Config and Managers
    config_file = "config/metadata_config.yaml"
    manager = MetadataManager(config_file)
    registry = SchemaRegistryService(manager)
    
    source_name = "test_dataset"
    
    print("\n--- Phase 1: Register Initial Dataset ---")
    # Initial Schema: 
    # - id (Integer)
    # - username (String)
    # - active (Boolean)
    # - score (Float)
    initial_fields = [
        MockField("id", "IntegerType()"),
        MockField("username", "StringType()"),
        MockField("active", "BooleanType()"),
        MockField("score", "FloatType()")
    ]
    df_v1 = MockDF(initial_fields)
    
    # Register the initial version
    registry.register_schema(source_name, df_v1)
    print(f"Registered initial schema for '{source_name}':")
    print(json.dumps(manager.get_latest_schema(source_name), indent=2))
    
    print("\n--- Phase 2: Simulate Schema Drift ---")
    # Drifted Schema:
    # - id (Integer)
    # - full_name (String)      <- Renamed point ('username' removed, 'full_name' added)
    # - active (Boolean)
    # - score (StringType)      <- Type change
    # - email (StringType)      <- New Column Added
    drifted_fields = [
        MockField("id", "IntegerType()"),
        MockField("full_name", "StringType()"),  # renaming username -> full_name
        MockField("active", "BooleanType()"),
        MockField("score", "StringType()"),      # change type FloatType -> StringType
        MockField("email", "StringType()")       # Added column
    ]
    df_v2 = MockDF(drifted_fields)
    
    print("Running Change Detection...")
    changes = registry.detect_changes(source_name, df_v2)
    
    print("\nDetected Changes:")
    print(json.dumps(changes, indent=2))
    
    print("\n--- Phase 3: Verify Log ---")
    log_file = os.path.join(os.path.dirname(__file__), "metadata_logs", "schema_change_log.json")
    if os.path.exists(log_file):
        with open(log_file, "r") as f:
            logs = json.load(f)
            print(f"Total Change Events Logged: {len(logs)}")
            print("\nLatest Log Entry:")
            print(json.dumps(logs[-1], indent=2))
    else:
        print("ERROR: Log file not found!")

if __name__ == "__main__":
    ROOT = os.path.dirname(os.path.abspath(__file__))

    # Ensure config path exists to avoid Manager crash
    os.makedirs(os.path.join(ROOT, "config"), exist_ok=True)
    config_path = os.path.join(ROOT, "config", "metadata_config.yaml")
    if not os.path.exists(config_path):
        with open(config_path, "w") as f:
            f.write("mock_config: true\n")

    # Clear out previous logs/schemas for a clean run
    schemas_dir = os.path.join(ROOT, "metadata_logs", "schemas")
    log_file    = os.path.join(ROOT, "metadata_logs", "schema_change_log.json")
    os.makedirs(schemas_dir, exist_ok=True)
    for fname in os.listdir(schemas_dir):
        os.remove(os.path.join(schemas_dir, fname))
    if os.path.exists(log_file):
        os.remove(log_file)

    main()
