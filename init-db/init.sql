-- Stores source/target table definitions
CREATE TABLE source_schemas (
    id SERIAL PRIMARY KEY,
    source_name VARCHAR(100),
    table_name VARCHAR(100),
    schema_version INT,
    schema_json JSONB,          -- full schema as JSON
    created_at TIMESTAMP,
    is_active BOOLEAN
);

-- Stores pipeline configurations
CREATE TABLE pipeline_configs (
    id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100),
    source_id INT REFERENCES source_schemas(id),
    target_table VARCHAR(100),
    transformation_rules JSONB,
    schedule_cron VARCHAR(50),
    is_active BOOLEAN
);

-- Stores lineage — what feeds what
CREATE TABLE lineage (
    id SERIAL PRIMARY KEY,
    pipeline_id INT REFERENCES pipeline_configs(id),
    upstream_table VARCHAR(100),
    downstream_table VARCHAR(100),
    dependency_type VARCHAR(50)
);

-- Stores detected schema changes
CREATE TABLE schema_change_log (
    id SERIAL PRIMARY KEY,
    source_id INT REFERENCES source_schemas(id),
    change_type VARCHAR(50),    -- 'column_added', 'type_changed' etc
    changed_at TIMESTAMP,
    old_schema JSONB,
    new_schema JSONB,
    resolved BOOLEAN DEFAULT FALSE
);