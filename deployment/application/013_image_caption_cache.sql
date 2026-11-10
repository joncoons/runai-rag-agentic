-- Image caption reuse grants no source-admission or publication authority.
CREATE SCHEMA IF NOT EXISTS application;
CREATE TABLE IF NOT EXISTS application.image_caption_cache (
 cache_key text PRIMARY KEY CHECK(length(cache_key)=64),
 collection_id text NOT NULL,
 raw_sha256 text NOT NULL CHECK(length(raw_sha256)=64),
 contract_sha256 text NOT NULL CHECK(length(contract_sha256)=64),
 status text NOT NULL CHECK(status IN ('running','ready')),
 owner uuid,
 fence bigint NOT NULL DEFAULT 1,
 lease_until timestamptz,
 artifact_uri text,
 artifact_sha256 text,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(collection_id,raw_sha256,contract_sha256),
 CHECK(status<>'ready' OR (artifact_uri IS NOT NULL AND length(artifact_sha256)=64))
);
CREATE TABLE IF NOT EXISTS application.image_caption_uses (
 document_id text NOT NULL,
 cache_key text NOT NULL REFERENCES application.image_caption_cache,
 artifact_sha256 text NOT NULL,
 cache_hit boolean NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(document_id,cache_key)
);
