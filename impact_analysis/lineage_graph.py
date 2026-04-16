import networkx as nx

class ImpactAnalyzer:
    def __init__(self, metadata_manager):
        self.meta = metadata_manager

    def build_lineage_graph(self):
        """Build a directed graph of table dependencies"""
        G = nx.DiGraph()
        for record in self.meta.get_all_lineage():
            G.add_edge(record['upstream_table'], record['downstream_table'])
        return G

    def get_impacted_components(self, changed_table):
        """Find all downstream components affected by a change"""
        G = self.build_lineage_graph()
        if changed_table not in G:
            return []
        return list(nx.descendants(G, changed_table))

    def compute_risk_score(self, changed_table):
        """Score impact based on depth and number of dependents"""
        G = self.build_lineage_graph()
        impacted = self.get_impacted_components(changed_table)

        if not impacted:
            return {
                'impacted_tables': [],
                'impact_count': 0,
                'max_depth': 0,
                'risk_score': 0,
                'severity': 'LOW'
            }

        depth = max(
            [nx.shortest_path_length(G, changed_table, t) for t in impacted],
            default=0
        )
        risk = len(impacted) * depth
        return {
            'impacted_tables': impacted,
            'impact_count': len(impacted),
            'max_depth': depth,
            'risk_score': risk,
            'severity': 'HIGH' if risk > 5 else 'MEDIUM' if risk > 2 else 'LOW'
        }