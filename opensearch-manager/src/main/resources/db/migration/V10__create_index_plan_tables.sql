-- V10: IndexPlan governance object — bundles VectorSet recipes + strategy +
-- HNSW + index settings into one row that materializes a physical OpenSearch
-- index. Referenced by opensearch-sink node configs via plan_ids[].

CREATE TABLE index_plan (
    id                   VARCHAR(255) PRIMARY KEY,
    name                 VARCHAR(255) NOT NULL,
    index_name           VARCHAR(255) NOT NULL,
    indexing_strategy    VARCHAR(64)  NOT NULL,

    -- HNSW knobs (nullable; null = use manager server-side default)
    hnsw_engine          VARCHAR(32),
    hnsw_method_name     VARCHAR(32),
    hnsw_space_type      VARCHAR(32),
    hnsw_m               INTEGER,
    hnsw_ef_construction INTEGER,
    hnsw_ef_search       INTEGER,

    -- Index-level settings (nullable; null = use manager server-side default)
    number_of_shards     INTEGER,
    number_of_replicas   INTEGER,
    refresh_interval     VARCHAR(32),
    knn_enabled          BOOLEAN,

    description          TEXT,

    -- Lifecycle: PENDING → READY (success) or FAILED (last_error populated)
    status               VARCHAR(32)  NOT NULL DEFAULT 'PENDING',
    last_error           TEXT,

    created_at           TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_index_plan_name UNIQUE (name)
);

CREATE INDEX idx_index_plan_status     ON index_plan(status);
CREATE INDEX idx_index_plan_index_name ON index_plan(index_name);

-- Plan ↔ VectorSet membership, ordered. Explicit join entity (rather than
-- @ElementCollection) so the row carries sort_order and can FK both sides.
CREATE TABLE index_plan_vector_set (
    plan_id        VARCHAR(255) NOT NULL,
    vector_set_id  VARCHAR(255) NOT NULL,
    sort_order     INTEGER      NOT NULL,

    PRIMARY KEY (plan_id, vector_set_id),

    CONSTRAINT unique_ipvs_plan_sort_order
        UNIQUE (plan_id, sort_order),

    CONSTRAINT fk_ipvs_plan
        FOREIGN KEY (plan_id) REFERENCES index_plan(id) ON DELETE CASCADE,

    CONSTRAINT fk_ipvs_vector_set
        FOREIGN KEY (vector_set_id) REFERENCES vector_set(id) ON DELETE RESTRICT
);

CREATE INDEX idx_ipvs_vector_set_id ON index_plan_vector_set(vector_set_id);
