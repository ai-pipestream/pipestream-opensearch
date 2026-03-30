-- VectorSet: canonical authority for the full semantic tuple
-- (chunker_config + embedding_model + index + field + result_set).
-- Supersedes IndexEmbeddingBinding as the source of truth.
-- IndexEmbeddingBinding remains for backward compatibility.

CREATE TABLE vector_set (
    id                          VARCHAR(255) PRIMARY KEY,
    name                        VARCHAR(255) NOT NULL,
    chunker_config_id           VARCHAR(255) NOT NULL,
    embedding_model_config_id   VARCHAR(255) NOT NULL,
    index_name                  VARCHAR(255) NOT NULL,
    field_name                  VARCHAR(255) NOT NULL,
    result_set_name             VARCHAR(255) NOT NULL DEFAULT 'default',
    source_field                VARCHAR(255) NOT NULL,
    vector_dimensions           INTEGER,
    metadata                    JSONB,
    created_at                  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at                  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_vector_set_name UNIQUE(name),
    CONSTRAINT unique_vector_set_index_field_result_set
        UNIQUE(index_name, field_name, result_set_name),

    CONSTRAINT fk_vector_set_chunker_config
        FOREIGN KEY (chunker_config_id) REFERENCES chunker_config(id),
    CONSTRAINT fk_vector_set_embedding_model_config
        FOREIGN KEY (embedding_model_config_id) REFERENCES embedding_model_config(id)
);

CREATE INDEX idx_vector_set_index_name ON vector_set(index_name);
CREATE INDEX idx_vector_set_chunker_config_id ON vector_set(chunker_config_id);
CREATE INDEX idx_vector_set_embedding_model_config_id ON vector_set(embedding_model_config_id);
CREATE INDEX idx_vector_set_created_at ON vector_set(created_at DESC);
