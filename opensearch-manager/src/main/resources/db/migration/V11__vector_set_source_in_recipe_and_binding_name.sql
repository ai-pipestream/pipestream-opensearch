-- V11: make source_cel part of vector-set identity, and let bindings be named.
--
-- (a) Recipe identity must include the embedded source. Two vector sets with
-- the same (field, result_set, chunker, embedding) but a different source_cel
-- embed DIFFERENT text, so they are DIFFERENT vector sets — previously they
-- collided on unique_vector_set_recipe and get-or-create silently returned the
-- wrong-source row. Replace the constraint with a unique index that also keys
-- on the source. source_cel is TEXT and can exceed btree's per-row size limit,
-- so index md5(source_cel) rather than the raw column.
--
-- Widening the key (adding a column) only ever permits MORE rows, so every row
-- that satisfied the old constraint still satisfies this one — safe on existing
-- data.
ALTER TABLE vector_set DROP CONSTRAINT IF EXISTS unique_vector_set_recipe;

CREATE UNIQUE INDEX unique_vector_set_recipe ON vector_set (
    field_name,
    result_set_name,
    chunker_config_id,
    embedding_model_config_id,
    md5(source_cel)
);

-- (b) Optional human-readable label for a deployment binding (e.g.
-- "data-science-title-binding"). Lets the same recipe deploy to an index under
-- a caller-meaningful name without denormalizing the recipe. Nullable.
ALTER TABLE vector_set_index_binding ADD COLUMN IF NOT EXISTS name VARCHAR(255);
