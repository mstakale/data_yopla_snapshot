-- Day 1: raw.products, the bronze landing table for Open Food Facts NL rows.
-- CREATE TABLE/INDEX IF NOT EXISTS so this is safe to re-run: it's picked up
-- automatically on a brand-new volume (docker-entrypoint-initdb.d), and
-- extract.py's load stage also executes this file directly on every run so
-- an already-initialised volume (like ours) gets the table too.
--
-- Raw is bronze/unvalidated by design (ELT): no NOT NULL beyond the two
-- pipeline-added columns, no PK/dedup here. Typing and dedup happen in dbt
-- staging (Day 3); freshness/completeness/format checks happen in Great
-- Expectations (Day 4).

CREATE TABLE IF NOT EXISTS raw.products (
    barcode text,
    product_name jsonb,
    brands text,
    categories_tags jsonb,
    nutriments jsonb,
    quantity text,
    nutriscore_grade text,
    last_modified_t bigint,
    countries_tags jsonb,
    product_quantity text,
    product_quantity_unit text,
    brands_tags jsonb,
    nova_group integer,
    environmental_score_grade text,
    completeness real,
    created_t bigint,
    labels_tags jsonb,
    data_quality_warnings_tags jsonb,
    unique_scans_n integer,
    nutrition_data_per text,
    load_date date NOT NULL,
    loaded_at timestamptz NOT NULL
);

-- Day 3C: added after the table already existed and had data - CREATE TABLE
-- IF NOT EXISTS above is a no-op against an existing table, so this ALTER is
-- what actually migrates it. Kept in the same file (not a separate migration
-- script) so extract.py's load stage - which re-runs this whole file every
-- run - stays the single source of truth for the schema either way.
ALTER TABLE raw.products ADD COLUMN IF NOT EXISTS nutrition_data_per text;

CREATE INDEX IF NOT EXISTS idx_products_load_date ON raw.products (load_date);
