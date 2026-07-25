DROP TABLE IF EXISTS metrics_series_activity_from_histograms_mv SYNC;
DROP TABLE IF EXISTS metrics_series_activity_from_samples_mv SYNC;
DROP TABLE IF EXISTS metrics_label_postings_from_label_index_mv SYNC;
DROP TABLE IF EXISTS metrics_label_postings_from_series_mv SYNC;
DROP TABLE IF EXISTS metrics_label_index_from_series_mv SYNC;
DROP TABLE IF EXISTS metrics_samples SYNC;
DROP TABLE IF EXISTS metrics_histograms SYNC;
DROP TABLE IF EXISTS metrics_exemplars SYNC;
DROP TABLE IF EXISTS metrics_metadata SYNC;
DROP TABLE IF EXISTS metrics_series_activity SYNC;
DROP TABLE IF EXISTS metrics_label_postings SYNC;
DROP TABLE IF EXISTS metrics_label_index SYNC;
DROP TABLE IF EXISTS metrics_series SYNC;

CREATE TABLE metrics_series
(
    team_id UInt64 CODEC(T64, Default),
    id UInt64,
    metric_name LowCardinality(String),
    labels_json String,
    min_time DateTime64(3, 'UTC') CODEC(DoubleDelta, Default),
    max_time DateTime64(3, 'UTC') CODEC(DoubleDelta, Default)
)
ENGINE = MergeTree
ORDER BY (team_id, metric_name, id)
SETTINGS index_granularity = 1024;

ALTER TABLE metrics_series
    ADD PROJECTION by_id
    (
        SELECT team_id, id, metric_name, labels_json, min_time, max_time
        ORDER BY (team_id, id)
    );

CREATE TABLE metrics_label_index
(
    team_id UInt64 CODEC(T64, Default),
    metric_name LowCardinality(String),
    label_name LowCardinality(String),
    label_value LowCardinality(String),
    id UInt64
)
ENGINE = ReplacingMergeTree
ORDER BY (team_id, metric_name, label_name, label_value, id)
SETTINGS index_granularity = 1024, deduplicate_merge_projection_mode = 'rebuild';

ALTER TABLE metrics_label_index
    ADD PROJECTION by_id_label
    (
        SELECT team_id, metric_name, label_name, label_value, id
        ORDER BY (team_id, id, label_name, metric_name, label_value)
    );

CREATE TABLE metrics_samples
(
    team_id UInt64 CODEC(T64, Default),
    metric_name LowCardinality(String),
    timestamp DateTime64(3, 'UTC') CODEC(DoubleDelta, Default),
    id UInt64,
    -- ZSTD beats Gorilla on real metric values: 85% of samples are exact
    -- integers, whose doubles share long byte patterns that ZSTD exploits and
    -- Gorilla's XOR destroys. Measured on 40M production samples: 1.374 ->
    -- 0.839 bytes/sample (1.64x smaller) and ~10% faster to scan. Existing
    -- tables: ALTER TABLE ... MODIFY COLUMN value Float64 CODEC(ZSTD(3)),
    -- which only re-encodes parts written after it unless you also rewrite.
    value Float64 CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (team_id, metric_name, toStartOfTenMinutes(timestamp), id, timestamp)
SETTINGS index_granularity = 8192;

CREATE TABLE metrics_histograms
(
    team_id UInt64 CODEC(T64, Default),
    metric_name LowCardinality(String),
    timestamp DateTime64(3, 'UTC') CODEC(DoubleDelta, Default),
    id UInt64,
    histogram String,
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (team_id, id, timestamp)
SETTINGS index_granularity = 1024;

CREATE TABLE metrics_exemplars
(
    team_id UInt64 CODEC(T64, Default),
    timestamp DateTime64(3, 'UTC') CODEC(DoubleDelta, Default),
    id UInt64,
    -- ZSTD beats Gorilla on real metric values: 85% of samples are exact
    -- integers, whose doubles share long byte patterns that ZSTD exploits and
    -- Gorilla's XOR destroys. Measured on 40M production samples: 1.374 ->
    -- 0.839 bytes/sample (1.64x smaller) and ~10% faster to scan. Existing
    -- tables: ALTER TABLE ... MODIFY COLUMN value Float64 CODEC(ZSTD(3)),
    -- which only re-encodes parts written after it unless you also rewrite.
    value Float64 CODEC(ZSTD(3)),
    labels_json String
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (team_id, id, timestamp)
SETTINGS index_granularity = 1024;

CREATE TABLE metrics_metadata
(
    team_id UInt64 CODEC(T64, Default),
    metric_family_name LowCardinality(String),
    type LowCardinality(String),
    unit String,
    help String,
    updated_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (team_id, metric_family_name)
SETTINGS index_granularity = 1024;

CREATE MATERIALIZED VIEW metrics_label_index_from_series_mv TO metrics_label_index AS
SELECT
    team_id,
    metric_name,
    tupleElement(label_pair, 1) AS label_name,
    tupleElement(label_pair, 2) AS label_value,
    id
FROM metrics_series
ARRAY JOIN JSONExtractKeysAndValues(labels_json, 'String') AS label_pair;
