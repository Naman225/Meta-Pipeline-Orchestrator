"""
tests/test_schema_registry.py
Unit tests for SchemaRegistryService — no Spark required.
"""
import os
import sys
import json
import tempfile
import shutil
import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, ROOT)

from metadata_manager.manager import MetadataManager
from schema_registry.registry import SchemaRegistryService


# ── Mock helpers ─────────────────────────────────────────────────────────────
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


# ── Fixtures ─────────────────────────────────────────────────────────────────
@pytest.fixture
def tmp_env(tmp_path):
    """Create a fully isolated temp project dir with config."""
    cfg = tmp_path / "config" / "metadata_config.yaml"
    cfg.parent.mkdir(parents=True)
    cfg.write_text("mock_config: true\n")
    (tmp_path / "metadata_logs" / "schemas").mkdir(parents=True)

    # Patch the manager's internal paths to use tmp_path
    import metadata_manager.manager as m_module
    original_logs = m_module._LOGS_DIR
    original_schemas = m_module._SCHEMAS_DIR
    m_module._LOGS_DIR    = str(tmp_path / "metadata_logs")
    m_module._SCHEMAS_DIR = str(tmp_path / "metadata_logs" / "schemas")
    (tmp_path / "metadata_logs" / "schemas").mkdir(parents=True, exist_ok=True)

    manager  = MetadataManager(str(cfg))
    registry = SchemaRegistryService(manager)
    yield manager, registry

    # Restore
    m_module._LOGS_DIR    = original_logs
    m_module._SCHEMAS_DIR = original_schemas


# ── Tests ─────────────────────────────────────────────────────────────────────
class TestSchemaRegistration:
    def test_register_and_retrieve(self, tmp_env):
        manager, registry = tmp_env
        fields = [MockField("id", "IntegerType()"), MockField("name", "StringType()")]
        registry.register_schema("src1", MockDF(fields))
        schema = manager.get_latest_schema("src1")
        assert schema == {"id": "IntegerType()", "name": "StringType()"}

    def test_register_empty_schema(self, tmp_env):
        manager, registry = tmp_env
        registry.register_schema("empty_src", MockDF([]))
        assert manager.get_latest_schema("empty_src") == {}

    def test_unknown_source_returns_empty(self, tmp_env):
        manager, _ = tmp_env
        assert manager.get_latest_schema("does_not_exist") == {}


class TestChangeDetection:
    BASE = [
        MockField("order_id",     "IntegerType()"),
        MockField("customer_id",  "StringType()"),
        MockField("total_price",  "FloatType()"),
    ]

    def _setup(self, tmp_env, source="orders"):
        manager, registry = tmp_env
        registry.register_schema(source, MockDF(self.BASE))
        return manager, registry

    def test_no_change(self, tmp_env):
        manager, registry = self._setup(tmp_env)
        changes = registry.detect_changes("orders", MockDF(self.BASE))
        assert changes == []

    def test_column_added(self, tmp_env):
        manager, registry = self._setup(tmp_env)
        v2 = self.BASE + [MockField("discount_pct", "FloatType()")]
        changes = registry.detect_changes("orders", MockDF(v2))
        added = [c for c in changes if c["type"] == "column_added"]
        assert len(added) == 1
        assert added[0]["column"] == "discount_pct"

    def test_column_removed(self, tmp_env):
        manager, registry = self._setup(tmp_env)
        v2 = [MockField("order_id", "IntegerType()"), MockField("customer_id", "StringType()")]
        changes = registry.detect_changes("orders", MockDF(v2))
        removed = [c for c in changes if c["type"] == "column_removed"]
        assert len(removed) == 1
        assert removed[0]["column"] == "total_price"

    def test_type_change(self, tmp_env):
        manager, registry = self._setup(tmp_env)
        v2 = [
            MockField("order_id",    "IntegerType()"),
            MockField("customer_id", "StringType()"),
            MockField("total_price", "StringType()"),   # Float → String
        ]
        changes = registry.detect_changes("orders", MockDF(v2))
        type_changes = [c for c in changes if c["type"] == "type_changed"]
        assert len(type_changes) == 1
        assert type_changes[0]["column"] == "total_price"
        assert type_changes[0]["old_type"] == "FloatType()"
        assert type_changes[0]["new_type"] == "StringType()"

    def test_rename_detected_as_remove_plus_add(self, tmp_env):
        manager, registry = self._setup(tmp_env)
        v2 = [
            MockField("order_id",      "IntegerType()"),
            MockField("client_id",     "StringType()"),   # renamed customer_id → client_id
            MockField("total_price",   "FloatType()"),
        ]
        changes = registry.detect_changes("orders", MockDF(v2))
        types = {c["type"] for c in changes}
        assert "column_removed" in types
        assert "column_added"   in types

    def test_multiple_changes_simultaneously(self, tmp_env):
        manager, registry = self._setup(tmp_env)
        v2 = [
            MockField("order_id",    "IntegerType()"),
            MockField("total_price", "StringType()"),   # type change
            MockField("status",      "StringType()"),   # new column
            # customer_id removed
        ]
        changes = registry.detect_changes("orders", MockDF(v2))
        assert len(changes) == 3  # 1 removed, 1 type_changed, 1 added


class TestChangeLog:
    def test_change_logged_on_drift(self, tmp_env):
        import metadata_manager.manager as m_module
        manager, registry = tmp_env
        fields_v1 = [MockField("id", "IntegerType()"), MockField("val", "FloatType()")]
        registry.register_schema("logged_src", MockDF(fields_v1))
        fields_v2 = [MockField("id", "IntegerType()"), MockField("val", "StringType()")]
        registry.detect_changes("logged_src", MockDF(fields_v2))

        log_path = os.path.join(m_module._LOGS_DIR, "schema_change_log.json")
        assert os.path.exists(log_path)
        with open(log_path) as f:
            logs = json.load(f)
        assert len(logs) >= 1
        assert logs[-1]["source_name"] == "logged_src"

    def test_no_log_on_no_change(self, tmp_env):
        import metadata_manager.manager as m_module
        manager, registry = tmp_env
        fields = [MockField("id", "IntegerType()")]
        registry.register_schema("stable_src", MockDF(fields))
        registry.detect_changes("stable_src", MockDF(fields))

        log_path = os.path.join(m_module._LOGS_DIR, "schema_change_log.json")
        assert not os.path.exists(log_path)
