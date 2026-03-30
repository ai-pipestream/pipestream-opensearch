-- Enforce NOT NULL on dimensions columns.
-- These must always be set from the embedding model configuration.

-- Clean up any orphaned rows that have NULL dimensions before adding constraints.
-- These rows are invalid and were created by bugs in the organic registration flow.
DELETE FROM vector_set_index_binding
WHERE vector_set_id IN (SELECT id FROM vector_set WHERE vector_dimensions IS NULL);

DELETE FROM vector_set WHERE vector_dimensions IS NULL;

-- Delete embedding configs with no dimensions — they are unusable.
-- First remove any referencing vector_sets and bindings.
DELETE FROM vector_set_index_binding
WHERE vector_set_id IN (
    SELECT vs.id FROM vector_set vs
    WHERE vs.embedding_model_config_id IN (
        SELECT id FROM embedding_model_config WHERE dimensions IS NULL
    )
);
DELETE FROM vector_set
WHERE embedding_model_config_id IN (
    SELECT id FROM embedding_model_config WHERE dimensions IS NULL
);
DELETE FROM index_embedding_binding
WHERE embedding_model_config_id IN (
    SELECT id FROM embedding_model_config WHERE dimensions IS NULL
);
DELETE FROM embedding_model_config WHERE dimensions IS NULL;

-- Now enforce NOT NULL
ALTER TABLE embedding_model_config ALTER COLUMN dimensions SET NOT NULL;
ALTER TABLE vector_set ALTER COLUMN vector_dimensions SET NOT NULL;
