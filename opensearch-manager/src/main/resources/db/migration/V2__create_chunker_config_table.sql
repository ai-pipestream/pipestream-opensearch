-- Chunker configurations (PostgreSQL)
-- Stores text chunking strategy configurations as JSON blobs for flexibility
CREATE TABLE chunker_config (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    config_id VARCHAR(255) NOT NULL,
    config_json JSONB NOT NULL,
    schema_ref VARCHAR(512),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB,
    CONSTRAINT unique_chunker_config_name UNIQUE(name),
    CONSTRAINT unique_chunker_config_id UNIQUE(config_id)
);

CREATE INDEX idx_chunker_config_config_id ON chunker_config(config_id);
CREATE INDEX idx_chunker_config_name ON chunker_config(name);
CREATE INDEX idx_chunker_config_created_at ON chunker_config(created_at DESC);
