class SchemaRegistryService:
    def __init__(self, metadata_manager):
        self.meta = metadata_manager

    def register_schema(self, source_name, df):
        """Infer schema from a DataFrame and store it"""
        schema = self._infer_schema(df)
        self.meta.register_schema(source_name, schema)

    def detect_changes(self, source_name, new_df):
        """Compare current data schema with registered version"""
        old_schema = self.meta.get_latest_schema(source_name)
        new_schema = self._infer_schema(new_df)
        
        changes = []
        
        # Check for added columns
        for col in new_schema:
            if col not in old_schema:
                changes.append({
                    'type': 'column_added',
                    'column': col,
                    'new_type': new_schema[col]
                })
        
        # Check for removed columns
        for col in old_schema:
            if col not in new_schema:
                changes.append({
                    'type': 'column_removed',
                    'column': col
                })
        
        # Check for type changes
        for col in old_schema:
            if col in new_schema and old_schema[col] != new_schema[col]:
                changes.append({
                    'type': 'type_changed',
                    'column': col,
                    'old_type': old_schema[col],
                    'new_type': new_schema[col]
                })
        
        if changes:
            self.meta.log_schema_change(source_name, changes, old_schema, new_schema)
            
            # Phase 6.2: Connect to Change Detection Engine and Orchestration
            try:
                import sys, os
                sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
                from impact_analysis.lineage_graph import ImpactAnalyzer
                
                analyzer = ImpactAnalyzer(self.meta)
                risk_assessment = analyzer.compute_risk_score(source_name)
                
                print("\n" + "="*50)
                print(" ORCHESTRATION LAYER DECISION ")
                print("="*50)
                print(f"Schema Change Detected for source: '{source_name}'")
                print(f"Impacted Downstream Components: { risk_assessment['impacted_tables'] }")
                print(f"Max Depth: { risk_assessment['max_depth'] }")
                print(f"Risk Score: { risk_assessment['risk_score'] } | Risk Severity: { risk_assessment['severity'] }")
                print("-" * 50)
                
                if risk_assessment['severity'] == 'LOW':
                    print("Action: AUTO-UPDATE -> Updating metadata and continuing pipeline.")
                elif risk_assessment['severity'] == 'MEDIUM':
                    print("Action: FLAG -> Flagging schema change for review. Pipeline continues cautiously.")
                else:
                    print("Action: PAUSE & ALERT -> HIGH RISK change detected! Pausing pipeline and sending alerts.")
                print("="*50 + "\n")
                
            except ImportError as e:
                print(f"Impact Analysis module not found or failed to load: {e}")
            except Exception as e:
                print(f"Could not compute impact analysis: {e}")
                
        return changes

    def _infer_schema(self, df):
        return {field.name: str(field.dataType) for field in df.schema.fields}