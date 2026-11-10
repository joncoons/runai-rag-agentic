-- Reusable generated vectors, never publication or deletion authority.
CREATE SCHEMA IF NOT EXISTS application;
CREATE TABLE IF NOT EXISTS application.legal_text_content (
 content_sha256 text PRIMARY KEY CHECK(content_sha256 ~ '^[0-9a-f]{64}$'),
 text text NOT NULL CHECK(length(text)>0),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS application.legal_text_embeddings (
 embedding_contract_sha256 text NOT NULL CHECK(embedding_contract_sha256 ~ '^[0-9a-f]{64}$'),
 content_sha256 text NOT NULL REFERENCES application.legal_text_content,
 vector double precision[] NOT NULL CHECK(array_length(vector,1)=2048),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(embedding_contract_sha256,content_sha256)
);
