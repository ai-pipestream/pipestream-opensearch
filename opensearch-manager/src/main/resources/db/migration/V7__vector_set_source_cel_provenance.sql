-- CEL source selector (TEXT), provenance, optional ownership, optional dedupe hash.

ALTER TABLE vector_set RENAME COLUMN source_field TO source_cel;

ALTER TABLE vector_set ALTER COLUMN source_cel TYPE TEXT;

ALTER TABLE vector_set ADD COLUMN IF NOT EXISTS provenance VARCHAR(32) NOT NULL DEFAULT 'REGISTERED';

ALTER TABLE vector_set ADD COLUMN IF NOT EXISTS owner_type VARCHAR(255);

ALTER TABLE vector_set ADD COLUMN IF NOT EXISTS owner_id VARCHAR(255);

ALTER TABLE vector_set ADD COLUMN IF NOT EXISTS content_signature VARCHAR(128);
