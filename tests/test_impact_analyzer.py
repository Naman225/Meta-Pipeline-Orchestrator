"""
tests/test_impact_analyzer.py
Unit tests for ImpactAnalyzer — no Spark / Docker required.
"""
import os
import sys
import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, ROOT)

from impact_analysis.lineage_graph import ImpactAnalyzer


# ── Mock metadata manager ─────────────────────────────────────────────────────
class MockManager:
    def __init__(self, edges):
        """edges = list of {upstream_table, downstream_table}"""
        self._edges = edges

    def get_all_lineage(self):
        return self._edges


# ── Tests ─────────────────────────────────────────────────────────────────────
class TestLineageGraph:
    def test_empty_graph(self):
        analyzer = ImpactAnalyzer(MockManager([]))
        G = analyzer.build_lineage_graph()
        assert G.number_of_nodes() == 0

    def test_simple_chain(self):
        edges = [
            {"upstream_table": "raw",     "downstream_table": "clean"},
            {"upstream_table": "clean",   "downstream_table": "feature"},
            {"upstream_table": "feature", "downstream_table": "model"},
        ]
        analyzer = ImpactAnalyzer(MockManager(edges))
        G = analyzer.build_lineage_graph()
        assert G.number_of_nodes() == 4
        assert G.number_of_edges() == 3

    def test_graph_direction(self):
        edges = [{"upstream_table": "A", "downstream_table": "B"}]
        analyzer = ImpactAnalyzer(MockManager(edges))
        G = analyzer.build_lineage_graph()
        assert G.has_edge("A", "B")
        assert not G.has_edge("B", "A")


class TestImpactComponents:
    def test_no_downstream(self):
        edges = [{"upstream_table": "A", "downstream_table": "B"}]
        analyzer = ImpactAnalyzer(MockManager(edges))
        impacted = analyzer.get_impacted_components("B")   # B is a leaf
        assert impacted == []

    def test_unknown_table(self):
        analyzer = ImpactAnalyzer(MockManager([]))
        assert analyzer.get_impacted_components("nonexistent") == []

    def test_all_downstream(self):
        edges = [
            {"upstream_table": "src",    "downstream_table": "clean"},
            {"upstream_table": "clean",  "downstream_table": "feature"},
            {"upstream_table": "feature","downstream_table": "dashboard"},
        ]
        analyzer = ImpactAnalyzer(MockManager(edges))
        impacted = set(analyzer.get_impacted_components("src"))
        assert impacted == {"clean", "feature", "dashboard"}

    def test_branching_lineage(self):
        edges = [
            {"upstream_table": "raw", "downstream_table": "clean_a"},
            {"upstream_table": "raw", "downstream_table": "clean_b"},
            {"upstream_table": "clean_a", "downstream_table": "model"},
        ]
        analyzer = ImpactAnalyzer(MockManager(edges))
        impacted = set(analyzer.get_impacted_components("raw"))
        assert "clean_a" in impacted
        assert "clean_b" in impacted
        assert "model"   in impacted


class TestRiskScore:
    def _chain(self, n):
        """Create a linear chain of n nodes."""
        tables = [f"t{i}" for i in range(n)]
        return [
            {"upstream_table": tables[i], "downstream_table": tables[i+1]}
            for i in range(n - 1)
        ]

    def test_no_impact_low_risk(self):
        analyzer = ImpactAnalyzer(MockManager([]))
        result = analyzer.compute_risk_score("standalone")
        assert result["severity"] == "LOW"
        assert result["risk_score"] == 0
        assert result["impacted_tables"] == []

    def test_single_hop_low_risk(self):
        edges = [{"upstream_table": "A", "downstream_table": "B"}]
        analyzer = ImpactAnalyzer(MockManager(edges))
        result = analyzer.compute_risk_score("A")
        # 1 impacted × depth 1 = score 1 → LOW
        assert result["severity"] == "LOW"
        assert result["risk_score"] == 1

    def test_medium_risk(self):
        # 3 nodes → score = 2 * 2 = 4 ... edge: need score >2, <=5
        edges = self._chain(4)  # t0→t1→t2→t3, 3 impacted, max_depth 3 → score 9 HIGH
        # Use 3-node chain: t0→t1→t2, 2 impacted, max_depth 2 → score 4 MEDIUM
        edges = self._chain(3)
        analyzer = ImpactAnalyzer(MockManager(edges))
        result = analyzer.compute_risk_score("t0")
        assert result["severity"] == "MEDIUM"

    def test_high_risk_long_chain(self):
        edges = self._chain(6)  # t0…t5: 5 impacted, depth 5, score 25 → HIGH
        analyzer = ImpactAnalyzer(MockManager(edges))
        result = analyzer.compute_risk_score("t0")
        assert result["severity"] == "HIGH"
        assert result["risk_score"] > 5

    def test_risk_score_formula(self):
        edges = self._chain(3)  # 2 impacted, max depth 2, score = 4
        analyzer = ImpactAnalyzer(MockManager(edges))
        result = analyzer.compute_risk_score("t0")
        assert result["impact_count"] == 2
        assert result["max_depth"]    == 2
        assert result["risk_score"]   == 4
