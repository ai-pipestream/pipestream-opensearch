-- V3: Allow multiple embeddings bindings per index + field when differentiated by result set.
-- Existing rows without a result_set_name are normalized to 'default' so behavior stays stable.
UPDATE index_embedding_binding
SET result_set_name = 'default'
WHERE result_set_name IS NULL OR btrim(result_set_name) = '';

ALTER TABLE index_embedding_binding
    ALTER COLUMN result_set_name SET DEFAULT 'default',
    ALTER COLUMN result_set_name SET NOT NULL;

ALTER TABLE index_embedding_binding
    DROP CONSTRAINT IF EXISTS unique_index_field;

ALTER TABLE index_embedding_binding
    ADD CONSTRAINT unique_index_field_result_set_name UNIQUE (index_name, field_name, result_set_name);
