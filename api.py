import sys
import os
import time
import json
from fastapi import FastAPI, UploadFile, File
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Add paths
ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, ROOT)

from metadata_manager.manager import MetadataManager
from schema_registry.registry import SchemaRegistryService
from impact_analysis.lineage_graph import ImpactAnalyzer
from change_detection.detector import ValidationDetector

app = FastAPI(title="Metadata Pipeline API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

CONFIG_FILE = os.path.join(ROOT, "config", "metadata_config.yaml")

class MockField:
    def __init__(self, name, dataType): self.name = name; self.dataType = dataType
    def to_dict(self): return {"name": self.name, "dataType": self.dataType}

class MockSchema:
    def __init__(self, fields): self.fields = fields

class MockDF:
    def __init__(self, fields): self.schema = MockSchema(fields)

def make_manager():
    os.makedirs(os.path.join(ROOT, "config"), exist_ok=True)
    if not os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "w") as f:
            f.write("mock_config: true\n")
    return MetadataManager(CONFIG_FILE)

def fresh_state(source_name: str):
    schemas_dir = os.path.join(ROOT, "metadata_logs", "schemas")
    os.makedirs(schemas_dir, exist_ok=True)
    log_file = os.path.join(ROOT, "metadata_logs", "schema_change_log.json")
    for path in [os.path.join(schemas_dir, f"{source_name}_schema.json"), log_file]:
        if os.path.exists(path):
            os.remove(path)

def _silent_detect(registry, source, fields):
    import io, contextlib
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        changes = registry.detect_changes(source, MockDF(fields))
    return changes

# Endpoints matching the scenarios
@app.get("/api/scenario/{scenario_id}")
def run_scenario(scenario_id: int):
    manager = make_manager()
    registry = SchemaRegistryService(manager)
    analyzer = ImpactAnalyzer(manager)
    
    response = {}
    
    if scenario_id == 1:
        # Add Column
        SOURCE = "orders_api_v1"
        fresh_state(SOURCE)
        v1 = [MockField("order_id","IntegerType()"), MockField("customer_id","StringType()"), MockField("total_price","FloatType()"), MockField("order_date","StringType()")]
        registry.register_schema(SOURCE, MockDF(v1))
        
        v2 = v1 + [MockField("discount_pct", "FloatType()")]
        t0 = time.monotonic()
        changes = _silent_detect(registry, SOURCE, v2)
        elapsed = (time.monotonic() - t0) * 1000
        impact = analyzer.compute_risk_score(SOURCE)
        
        response = {
            "title": "Scenario 1: Add a Column",
            "before": [f.to_dict() for f in v1],
            "after": [f.to_dict() for f in v2],
            "changes": changes,
            "impact": impact,
            "elapsed_ms": round(elapsed, 1),
            "resolution": "AUTO-UPDATE (LOW risk)" if impact["severity"] == "LOW" else "FLAG"
        }
        
    elif scenario_id == 2:
        # Rename Field
        SOURCE = "orders_api_v2"
        fresh_state(SOURCE)
        v1 = [MockField("order_id","IntegerType()"), MockField("cust_name","StringType()"), MockField("total_price","FloatType()")]
        registry.register_schema(SOURCE, MockDF(v1))
        
        v2 = [MockField("order_id","IntegerType()"), MockField("customer_name","StringType()"), MockField("total_price","FloatType()")]
        t0 = time.monotonic()
        changes = _silent_detect(registry, SOURCE, v2)
        elapsed = (time.monotonic() - t0) * 1000
        impact = analyzer.compute_risk_score(SOURCE)
        
        response = {
            "title": "Scenario 2: Rename a Field",
            "before": [f.to_dict() for f in v1],
            "after": [f.to_dict() for f in v2],
            "changes": changes,
            "impact": impact,
            "elapsed_ms": round(elapsed, 1),
            "resolution": "FLAG (MEDIUM risk)"
        }
        
    elif scenario_id == 3:
        # Change Data Type
        SOURCE = "orders_api_v3"
        fresh_state(SOURCE)
        v1 = [MockField("order_id","IntegerType()"), MockField("total_price","FloatType()"), MockField("order_date","DateType()")]
        registry.register_schema(SOURCE, MockDF(v1))
        
        v2 = [MockField("order_id","IntegerType()"), MockField("total_price","StringType()"), MockField("order_date","DateType()")]
        t0 = time.monotonic()
        changes = _silent_detect(registry, SOURCE, v2)
        elapsed = (time.monotonic() - t0) * 1000
        impact = analyzer.compute_risk_score(SOURCE)
        
        response = {
            "title": "Scenario 3: Change Data Type (Regression)",
            "before": [f.to_dict() for f in v1],
            "after": [f.to_dict() for f in v2],
            "changes": changes,
            "impact": impact,
            "elapsed_ms": round(elapsed, 1),
            "resolution": "PAUSE & ALERT (HIGH risk)"
        }
        
    elif scenario_id == 4:
        # Null values
        SOURCE = "orders_api_v4"
        fresh_state(SOURCE)
        v1 = [MockField("order_id","IntegerType()"), MockField("customer_id","StringType()"), MockField("total_price","FloatType()")]
        registry.register_schema(SOURCE, MockDF(v1))
        
        dataset = [
            {"order_id": 1, "customer_id": "C001", "total_price": 173.65},
            {"order_id": 2, "customer_id": "C002", "total_price": None},
            {"order_id": 3, "customer_id": "C003", "total_price": 48.20},
        ]
        
        t0 = time.monotonic()
        detector = ValidationDetector()
        results = detector.run_all(dataset, required_columns=["total_price", "customer_id"], type_checks={"total_price": float})
        elapsed = (time.monotonic() - t0) * 1000
        null_res = results[0]
        
        response = {
            "title": "Scenario 4: Introduce Missing Values",
            "before": [f.to_dict() for f in v1],
            "after": [f.to_dict() for f in v1],
            "changes": [{"type": "validation_failure", "column": "total_price", "details": f"{null_res['rows_affected']} rows contain nulls"}],
            "impact": {"impact_count": 1, "severity": "MEDIUM" if null_res["triggered"] else "LOW"},
            "elapsed_ms": round(elapsed, 1),
            "resolution": "FLAG (Validation Failed)"
        }

    return response


@app.post("/api/upload_dataset")
async def upload_dataset(file: UploadFile = File(...)):
    manager = make_manager()
    registry = SchemaRegistryService(manager)
    analyzer = ImpactAnalyzer(manager)
    
    # Read the file
    content = await file.read()
    source_name = os.path.splitext(file.filename)[0].replace(" ", "_").lower()
    
    temp_path = f"/tmp/{file.filename}"
    with open(temp_path, "wb") as f:
        f.write(content)
        
    try:
        import pandas as pd
        if file.filename.endswith(".json"):
            df = pd.read_json(temp_path)
        else:
            df = pd.read_csv(temp_path)
            
        dtype_map = {
            "int64": "IntegerType()",
            "float64": "FloatType()",
            "object": "StringType()",
            "bool": "BooleanType()"
        }
        
        fields = []
        for col, dtype in df.dtypes.items():
            mapped_type = dtype_map.get(str(dtype), "StringType()")
            # Check for generic dates intuitively
            if "date" in str(col).lower() or "time" in str(col).lower():
                mapped_type = "DateType()"
            fields.append(MockField(col, mapped_type))
            
    except Exception as e:
        return {"error": f"Failed to parse file: {str(e)}"}

    existing_schema = manager.get_latest_schema(source_name)
    before_fields = []
    
    t0 = time.monotonic()
    
    if not existing_schema:
        # Phase 1: Onboard dataset
        registry.register_schema(source_name, MockDF(fields))
        changes = [{"type": "column_added", "column": f.name} for f in fields]
        impact = {"impact_count": 0, "severity": "LOW"}
        resolution = "AUTO-UPDATE (New Source Registered)"
    else:
        # Drift Detection on subsequent uploads
        for f in existing_schema.get("fields", []):
            before_fields.append(MockField(f["name"], f["dataType"]))
            
        changes = _silent_detect(registry, source_name, fields)
        impact = analyzer.compute_risk_score(source_name)
        if impact["severity"] == "LOW": resolution = "AUTO-UPDATE (LOW risk)"
        elif impact["severity"] == "MEDIUM": resolution = "FLAG (MEDIUM risk)"
        else: resolution = "PAUSE & ALERT (HIGH risk)"
        
    elapsed = (time.monotonic() - t0) * 1000
    
    return {
        "title": f"Custom Dataset Analytics: {file.filename}",
        "before": [f.to_dict() for f in before_fields],
        "after": [f.to_dict() for f in fields],
        "changes": changes,
        "impact": impact,
        "elapsed_ms": round(elapsed, 1),
        "resolution": resolution
    }

# Mount static dashboard route
dashboard_dir = os.path.join(ROOT, "dashboard")
os.makedirs(dashboard_dir, exist_ok=True)
app.mount("/", StaticFiles(directory=dashboard_dir, html=True), name="dashboard")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)
