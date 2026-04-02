-- V8: Add semantic_config table and modify vector_set for semantic path.
--
-- semantic_config stores semantic chunking parameters. On registration,
-- child VectorSets are eagerly created for each enabled granularity.
-- vector_set gains nullable semantic_config_id + granularity columns.
-- A CHECK constraint enforces: exactly one of chunker_config_id or
-- semantic_config_id must be non-null.

CREATE TABLE semantic_config (
    id                          VARCHAR(255) PRIMARY KEY,
    name                        VARCHAR(255) NOT NULL UNIQUE,
    config_id                   VARCHAR(255) NOT NULL UNIQUE,
    embedding_model_id          VARCHAR(255) NOT NULL
        REFERENCES embedding_model_config(id),
    similarity_threshold        REAL NOT NULL DEFAULT 0.5,
    percentile_threshold        INTEGER NOT NULL DEFAULT 20,
    min_chunk_sentences         INTEGER NOT NULL DEFAULT 2,
    max_chunk_sentences         INTEGER NOT NULL DEFAULT 30,
    store_sentence_vectors      BOOLEAN NOT NULL DEFAULT TRUE,
    compute_centroids           BOOLEAN NOT NULL DEFAULT TRUE,
    config_json                 JSONB,
    source_cel                  TEXT NOT NULL DEFAULT 'document.search_metadata.body',
    created_at                  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at                  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_semantic_config_embedding_model ON semantic_config(embedding_model_id);

-- Make chunker_config_id nullable for semantic VectorSets
ALTER TABLE vector_set ALTER COLUMN chunker_config_id DROP NOT NULL;

-- Add semantic path columns
ALTER TABLE vector_set ADD COLUMN semantic_config_id VARCHAR(255)
    REFERENCES semantic_config(id);
ALTER TABLE vector_set ADD COLUMN granularity VARCHAR(50);

-- Enforce: exactly one path (standard OR semantic)
ALTER TABLE vector_set ADD CONSTRAINT chk_vector_set_type CHECK (
    (chunker_config_id IS NOT NULL AND semantic_config_id IS NULL AND granularity IS NULL)
    OR
    (chunker_config_id IS NULL AND semantic_config_id IS NOT NULL AND granularity IS NOT NULL)
);

CREATE INDEX idx_vector_set_semantic_config ON vector_set(semantic_config_id);
CREATE INDEX idx_vector_set_granularity ON vector_set(granularity);
