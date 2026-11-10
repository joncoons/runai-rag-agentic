-- Pure transform acknowledgments only: no collection, ACL or publication authority.
CREATE TABLE IF NOT EXISTS collection_control.html_normalizations (
 raw_sha256 text NOT NULL CHECK(raw_sha256 ~ '^[0-9a-f]{64}$'),
 normalizer_sha256 text NOT NULL CHECK(normalizer_sha256 ~ '^[0-9a-f]{64}$'),
 units_sha256 text NOT NULL CHECK(units_sha256 ~ '^[0-9a-f]{64}$'),
 verified_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(raw_sha256,normalizer_sha256)
);
