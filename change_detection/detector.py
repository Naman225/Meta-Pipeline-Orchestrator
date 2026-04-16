"""
Change Detection Engine — Detector Module
Handles validation rule triggers, specifically for missing value detection.
"""
import logging

logger = logging.getLogger(__name__)


class ValidationDetector:
    """
    Runs validation rules against a dataset (represented as a list of dicts)
    and flags any violations.
    """

    def __init__(self, rules: list = None):
        self.rules = rules or []

    def check_nulls(self, data: list, required_columns: list) -> dict:
        """
        Check if any required columns contain missing/null values.

        Args:
            data: List of row dicts (mock dataset).
            required_columns: Columns that must not be null.

        Returns:
            A result dict with: triggered (bool), violations (list), rows_affected (int).
        """
        violations = []
        rows_affected = set()

        for i, row in enumerate(data):
            for col in required_columns:
                val = row.get(col)
                if val is None or str(val).strip().lower() in ("null", "none", ""):
                    violations.append({
                        "row_index": i,
                        "column": col,
                        "value": val
                    })
                    rows_affected.add(i)

        return {
            "triggered": len(violations) > 0,
            "violations": violations,
            "rows_affected": len(rows_affected),
            "total_rows": len(data),
            "rule": "NOT_NULL",
            "required_columns": required_columns
        }

    def check_type_consistency(self, data: list, col: str, expected_type: type) -> dict:
        """
        Check if a column's values are all of the expected type.
        """
        violations = []
        for i, row in enumerate(data):
            val = row.get(col)
            if val is not None and not isinstance(val, expected_type):
                try:
                    expected_type(val)
                except (ValueError, TypeError):
                    violations.append({"row_index": i, "column": col, "value": val})

        return {
            "triggered": len(violations) > 0,
            "violations": violations,
            "rule": "TYPE_CONSISTENCY",
            "column": col,
            "expected_type": expected_type.__name__
        }

    def run_all(self, data: list, required_columns: list, type_checks: dict = None) -> list:
        """
        Run all validation rules and return aggregated results.

        Args:
            data: Mock dataset.
            required_columns: Columns that must not be null.
            type_checks: Dict of col -> type, e.g. {'price': float}.

        Returns:
            List of violation report dicts.
        """
        results = []
        null_result = self.check_nulls(data, required_columns)
        results.append(null_result)

        if type_checks:
            for col, expected_type in type_checks.items():
                results.append(self.check_type_consistency(data, col, expected_type))

        return results
