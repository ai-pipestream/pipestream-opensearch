-- Embedding model configurations (PostgreSQL)
-- Stores metadata about embedding models: dimensions, name, model identifier
CREATE TABLE embedding_model_config (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    model_identifier VARCHAR(512) NOT NULL,
    dimensions INTEGER,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB,
    CONSTRAINT unique_embedding_model_name UNIQUE(name)
);

-- Index-embedding bindings: which OpenSearch index uses which embedding config for which field
CREATE TABLE index_embedding_binding (
    id VARCHAR(255) PRIMARY KEY,
    index_name VARCHAR(255) NOT NULL,
    embedding_model_config_id VARCHAR(255) NOT NULL,
    field_name VARCHAR(255) NOT NULL,
    result_set_name VARCHAR(255),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_embedding_model FOREIGN KEY (embedding_model_config_id) REFERENCES embedding_model_config(id) ON DELETE CASCADE,
    CONSTRAINT unique_index_field UNIQUE(index_name, field_name)
);

CREATE INDEX idx_index_embedding_binding_index ON index_embedding_binding(index_name);
CREATE INDEX idx_index_embedding_binding_model ON index_embedding_binding(embedding_model_config_id);
