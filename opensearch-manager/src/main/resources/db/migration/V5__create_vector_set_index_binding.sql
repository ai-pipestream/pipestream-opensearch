-- V5: Decouple VectorSet (Recipe) from Index Deployment (Binding)
-- 1. Create the join table for deployment tracking
CREATE TABLE vector_set_index_binding (
    id              VARCHAR(255) PRIMARY KEY,
    vector_set_id   VARCHAR(255) NOT NULL,
    index_name      VARCHAR(255) NOT NULL,
    account_id      VARCHAR(255),
    datasource_id   VARCHAR(255),
    status          VARCHAR(50),
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_vs_index_binding
        UNIQUE(vector_set_id, index_name),

    CONSTRAINT fk_vs_binding_vector_set
        FOREIGN KEY (vector_set_id) REFERENCES vector_set(id) ON DELETE CASCADE
);

CREATE INDEX idx_vs_binding_index_name ON vector_set_index_binding(index_name);
CREATE INDEX idx_vs_binding_account_id ON vector_set_index_binding(account_id);
CREATE INDEX idx_vs_binding_datasource_id ON vector_set_index_binding(datasource_id);

-- 2. Modify vector_set to remove the per-index constraint and column
-- We first remove the index column and its constraints/indices
DROP INDEX IF EXISTS idx_vector_set_index_name;
ALTER TABLE vector_set DROP CONSTRAINT IF EXISTS unique_vector_set_index_field_result_set;
ALTER TABLE vector_set DROP COLUMN index_name;

-- 3. Add the recipe-based unique constraint to vector_set
ALTER TABLE vector_set ADD CONSTRAINT unique_vector_set_recipe
    UNIQUE(field_name, result_set_name, chunker_config_id, embedding_model_config_id);
