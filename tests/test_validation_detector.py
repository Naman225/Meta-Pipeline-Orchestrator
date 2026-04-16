"""
tests/test_validation_detector.py
Unit tests for ValidationDetector (change_detection/detector.py).
"""
import os
import sys
import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from change_detection.detector import ValidationDetector


# ── Sample datasets ───────────────────────────────────────────────────────────
CLEAN_DATA = [
    {"order_id": 1, "customer_id": "C001", "total_price": 99.99},
    {"order_id": 2, "customer_id": "C002", "total_price": 48.50},
    {"order_id": 3, "customer_id": "C003", "total_price": 12.00},
]

NULL_DATA = [
    {"order_id": 1, "customer_id": "C001", "total_price": 99.99},
    {"order_id": 2, "customer_id": "C002", "total_price": None},      # None
    {"order_id": 3, "customer_id": None,   "total_price": 12.00},     # None
    {"order_id": 4, "customer_id": "C004", "total_price": "null"},    # string null
    {"order_id": 5, "customer_id": "C005", "total_price": ""},        # empty string
]


class TestCheckNulls:
    def test_clean_data_no_violations(self):
        detector = ValidationDetector()
        result = detector.check_nulls(CLEAN_DATA, ["order_id", "customer_id", "total_price"])
        assert result["triggered"] is False
        assert result["violations"] == []
        assert result["rows_affected"] == 0

    def test_detects_none_value(self):
        detector = ValidationDetector()
        result = detector.check_nulls(NULL_DATA, ["total_price"])
        assert result["triggered"] is True
        # row index 1 (None), 3 ("null"), 4 ("")
        assert result["rows_affected"] == 3

    def test_detects_string_null(self):
        detector = ValidationDetector()
        result = detector.check_nulls(NULL_DATA, ["total_price"])
        values = [v["value"] for v in result["violations"]]
        assert None in values
        assert "null" in values

    def test_detects_empty_string(self):
        detector = ValidationDetector()
        result = detector.check_nulls(NULL_DATA, ["total_price"])
        values = [v["value"] for v in result["violations"]]
        assert "" in values

    def test_multiple_required_columns(self):
        detector = ValidationDetector()
        result = detector.check_nulls(NULL_DATA, ["customer_id", "total_price"])
        assert result["triggered"] is True
        # row 1 (total_price=None), row 2 (customer_id=None), row 3, row 4
        assert result["rows_affected"] >= 3

    def test_empty_dataset(self):
        detector = ValidationDetector()
        result = detector.check_nulls([], ["order_id"])
        assert result["triggered"] is False
        assert result["total_rows"] == 0

    def test_total_rows_count(self):
        detector = ValidationDetector()
        result = detector.check_nulls(CLEAN_DATA, ["order_id"])
        assert result["total_rows"] == 3


class TestCheckTypeConsistency:
    def test_all_correct_types(self):
        detector = ValidationDetector()
        result = detector.check_type_consistency(CLEAN_DATA, "total_price", float)
        assert result["triggered"] is False

    def test_wrong_type_detected(self):
        bad_data = [
            {"order_id": 1, "total_price": 99.99},
            {"order_id": 2, "total_price": "not_a_float"},  # bad
        ]
        detector = ValidationDetector()
        result = detector.check_type_consistency(bad_data, "total_price", float)
        assert result["triggered"] is True
        assert result["violations"][0]["value"] == "not_a_float"

    def test_none_skipped(self):
        """None values should be skipped (not flagged as type violations)."""
        data = [{"val": None}, {"val": 1.0}]
        detector = ValidationDetector()
        result = detector.check_type_consistency(data, "val", float)
        assert result["triggered"] is False

    def test_expected_type_name_in_result(self):
        detector = ValidationDetector()
        result = detector.check_type_consistency(CLEAN_DATA, "total_price", float)
        assert result["expected_type"] == "float"


class TestRunAll:
    def test_run_all_no_issues(self):
        detector = ValidationDetector()
        results = detector.run_all(CLEAN_DATA,
                                   required_columns=["order_id", "customer_id"],
                                   type_checks={"total_price": float})
        for r in results:
            assert r["triggered"] is False

    def test_run_all_with_nulls(self):
        detector = ValidationDetector()
        results = detector.run_all(NULL_DATA, required_columns=["total_price"])
        null_result = results[0]
        assert null_result["triggered"] is True

    def test_run_all_returns_list(self):
        detector = ValidationDetector()
        results = detector.run_all(CLEAN_DATA, required_columns=["order_id"])
        assert isinstance(results, list)
        assert len(results) >= 1

    def test_run_all_with_type_checks(self):
        detector = ValidationDetector()
        results = detector.run_all(
            CLEAN_DATA,
            required_columns=["order_id"],
            type_checks={"total_price": float, "order_id": int}
        )
        # Should have 1 null-check + 2 type-check results = 3 total
        assert len(results) == 3
