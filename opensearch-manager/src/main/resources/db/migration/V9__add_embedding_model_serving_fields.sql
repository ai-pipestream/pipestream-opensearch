-- V9: Add serving configuration fields to embedding_model_config for DB-driven model registry.
-- These fields replace the hardcoded EmbeddingModel enum and DjlModelConfig in module-embedder.

ALTER TABLE embedding_model_config
    ADD COLUMN IF NOT EXISTS endpoint_url VARCHAR(512) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS serving_name VARCHAR(255) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS query_prefix VARCHAR(255) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS index_prefix VARCHAR(255) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT true,
    ADD COLUMN IF NOT EXISTS tls_config_name VARCHAR(255),
    ADD COLUMN IF NOT EXISTS provider VARCHAR(100) NOT NULL DEFAULT 'djl-serving';

-- Seed default embedding models.
-- endpoint_url uses http://djl-serving:8080 as default base (Docker service name).
-- Override via gRPC UpdateEmbeddingModelConfig at deployment time for real endpoints.
INSERT INTO embedding_model_config (id, name, model_identifier, dimensions, endpoint_url, serving_name, query_prefix, index_prefix, enabled, provider, created_at, updated_at)
VALUES
    ('emb-minilm-l6-v2',           'ALL_MINILM_L6_V2',                          'sentence-transformers/all-MiniLM-L6-v2',                    384,  'http://djl-serving:8080/predictions/minilm',                                  'minilm',                                  '', '', true, 'djl-serving', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('emb-mpnet-base-v2',          'ALL_MPNET_BASE_V2',                         'sentence-transformers/all-mpnet-base-v2',                   768,  'http://djl-serving:8080/predictions/mpnet',                                   'mpnet',                                   '', '', true, 'djl-serving', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('emb-distilroberta-v1',       'ALL_DISTILROBERTA_V1',                      'sentence-transformers/all-distilroberta-v1',                768,  'http://djl-serving:8080/predictions/all_distilroberta_v1',                    'all_distilroberta_v1',                    '', '', true, 'djl-serving', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('emb-paraphrase-minilm-l3',   'PARAPHRASE_MINILM_L3_V2',                  'sentence-transformers/paraphrase-MiniLM-L3-v2',             384,  'http://djl-serving:8080/predictions/paraphrase_MiniLM_L3_v2',                 'paraphrase_MiniLM_L3_v2',                 '', '', true, 'djl-serving', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('emb-multilingual-minilm',    'PARAPHRASE_MULTILINGUAL_MINILM_L12_V2',    'sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2', 384, 'http://djl-serving:8080/predictions/paraphrase_multilingual_MiniLM_L12_v2',  'paraphrase_multilingual_MiniLM_L12_v2',   '', '', true, 'djl-serving', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('emb-e5-small-v2',            'E5_SMALL_V2',                               'intfloat/e5-small-v2',                                      384,  'http://djl-serving:8080/predictions/e5_small_v2',                             'e5_small_v2',                             'query: ', 'passage: ', true, 'djl-serving', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('emb-e5-large-v2',            'E5_LARGE_V2',                               'intfloat/e5-large-v2',                                     1024,  'http://djl-serving:8080/predictions/e5_large_v2',                             'e5_large_v2',                             'query: ', 'passage: ', true, 'djl-serving', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('emb-multi-qa-minilm',        'MULTI_QA_MINILM_L6_COS_V1',                'sentence-transformers/multi-qa-MiniLM-L6-cos-v1',           384,  'http://djl-serving:8080/predictions/multi_qa_MiniLM_L6_cos_v1',              'multi_qa_MiniLM_L6_cos_v1',               '', '', true, 'djl-serving', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('emb-bge-m3',                 'BGE_M3',                                     'BAAI/bge-m3',                                              1024,  'http://djl-serving:8080/predictions/bge_m3',                                  'bge_m3',                                  '', '', true, 'djl-serving', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT (id) DO NOTHING;
