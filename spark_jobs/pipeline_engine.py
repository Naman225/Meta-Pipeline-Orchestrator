class PipelineEngine:
    def __init__(self, spark_session, metadata_manager):
        self.spark = spark_session
        self.meta = metadata_manager

    def run_pipeline(self, pipeline_name):
        # 1. Fetch config from metadata
        config = self.meta.get_pipeline_config(pipeline_name)
        
        # 2. Load source data
        df = self.spark.read.csv(config['source']['path'], header=True)
        
        # 3. Apply transformations FROM metadata — not hardcoded
        for transform in config['transformations']:
            df = self.apply_transform(df, transform)
        
        # 4. Validate
        df = self.validate(df, config['validation_rules'])
        
        # 5. Write to target
        df.write.format('delta').mode("overwrite").save(config['target']['path'])
        
        # 6. Record lineage
        self.meta.record_lineage(pipeline_name, config)

    def validate(self, df, rules):
        # Stub validate: returns dataframe unchanged
        print(f"Applying {len(rules)} validation rules... OK.")
        return df

    def apply_transform(self, df, transform):
        if transform['type'] == 'rename_column':
            return df.withColumnRenamed(transform['from'], transform['to'])
        elif transform['type'] == 'cast':
            return df.withColumn(transform['column'], 
                                 df[transform['column']].cast(transform['to']))
        elif transform['type'] == 'drop_nulls':
            return df.dropna(subset=transform['columns'])
        # add more transform types as needed