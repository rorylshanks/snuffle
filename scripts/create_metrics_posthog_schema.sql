DROP VIEW IF EXISTS metrics SYNC;
DROP TABLE IF EXISTS metrics1_to_resource_attributes SYNC;
DROP TABLE IF EXISTS metrics1_to_metric_attributes SYNC;
DROP TABLE IF EXISTS metrics1 SYNC;
DROP TABLE IF EXISTS metric_attributes SYNC;

CREATE TABLE metric_attributes
(
    team_id Int32 CODEC(DoubleDelta, ZSTD(1)),
    time_bucket DateTime64(0, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    service_name LowCardinality(String) CODEC(ZSTD(1)),
    resource_fingerprint UInt64 DEFAULT 0 CODEC(DoubleDelta, ZSTD(1)),
    attribute_key LowCardinality(String) CODEC(ZSTD(1)),
    attribute_value String CODEC(ZSTD(1)),
    attribute_count SimpleAggregateFunction(sum, UInt64),
    attribute_type LowCardinality(String),
    INDEX idx_attribute_key attribute_key TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attribute_value attribute_value TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attribute_key_n3 attribute_key TYPE ngrambf_v1(3, 32768, 3, 0) GRANULARITY 1,
    INDEX idx_attribute_value_n3 attribute_value TYPE ngrambf_v1(3, 32768, 3, 0) GRANULARITY 1
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(time_bucket)
ORDER BY (team_id, attribute_type, time_bucket, resource_fingerprint, attribute_key, attribute_value)
SETTINGS
    deduplicate_merge_projection_mode = 'drop',
    index_granularity = 8192;

CREATE TABLE metrics1
(
    time_bucket DateTime MATERIALIZED toStartOfDay(timestamp) CODEC(DoubleDelta, ZSTD(1)),
    uuid String CODEC(ZSTD(1)),
    team_id Int32 CODEC(ZSTD(1)),
    trace_id String CODEC(ZSTD(1)),
    span_id String CODEC(ZSTD(1)),
    trace_flags Int32 CODEC(ZSTD(1)),
    timestamp DateTime64(6, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    observed_timestamp DateTime64(6, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    created_at DateTime64(6, 'UTC') MATERIALIZED now64(6) CODEC(DoubleDelta, ZSTD(1)),
    service_name LowCardinality(String) CODEC(ZSTD(1)),
    metric_name LowCardinality(String) CODEC(ZSTD(1)),
    metric_type LowCardinality(String) CODEC(ZSTD(1)),
    value Float64 CODEC(ZSTD(3)),
    count UInt64 DEFAULT 1 CODEC(T64, ZSTD(1)),
    histogram_bounds Array(Float64) CODEC(ZSTD(1)),
    histogram_counts Array(UInt64) CODEC(ZSTD(1)),
    unit LowCardinality(String) CODEC(ZSTD(1)),
    aggregation_temporality LowCardinality(String) CODEC(ZSTD(1)),
    is_monotonic Bool DEFAULT false CODEC(ZSTD(1)),
    resource_attributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    resource_fingerprint UInt64 MATERIALIZED cityHash64(resource_attributes) CODEC(DoubleDelta, ZSTD(1)),
    instrumentation_scope String CODEC(ZSTD(1)),
    attributes_map_str Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    attributes_map_float Map(LowCardinality(String), Float64) CODEC(ZSTD(1)),
    time_minute DateTime ALIAS toStartOfMinute(timestamp),
    attributes Map(String, String) ALIAS mapApply((k, v) -> (left(k, -5), v), attributes_map_str),
    INDEX idx_metric_name_set metric_name TYPE set(100) GRANULARITY 1,
    INDEX idx_metric_type_set metric_type TYPE set(10) GRANULARITY 1,
    INDEX idx_attributes_str_keys mapKeys(attributes_map_str) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attributes_str_values mapValues(attributes_map_str) TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_observed_minmax observed_timestamp TYPE minmax GRANULARITY 1,
    PROJECTION projection_aggregate_counts
    (
        SELECT
            team_id,
            time_bucket,
            toStartOfMinute(timestamp),
            service_name,
            metric_name,
            metric_type,
            resource_fingerprint,
            count() AS event_count,
            sum(value) AS total_value,
            min(value) AS min_value,
            max(value) AS max_value
        GROUP BY
            team_id,
            time_bucket,
            toStartOfMinute(timestamp),
            service_name,
            metric_name,
            metric_type,
            resource_fingerprint
    )
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
PRIMARY KEY (team_id, time_bucket, service_name, metric_name, resource_fingerprint, timestamp)
ORDER BY (team_id, time_bucket, service_name, metric_name, resource_fingerprint, timestamp)
SETTINGS
    index_granularity_bytes = 104857600,
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE VIEW metrics AS SELECT * FROM metrics1;

CREATE MATERIALIZED VIEW metrics1_to_metric_attributes TO metric_attributes
(
    team_id Int32,
    time_bucket DateTime64(0, 'UTC'),
    service_name LowCardinality(String),
    resource_fingerprint UInt64,
    attribute_key LowCardinality(String),
    attribute_value String,
    attribute_type LowCardinality(String),
    attribute_count SimpleAggregateFunction(sum, UInt64)
)
AS SELECT
    team_id,
    time_bucket,
    service_name,
    resource_fingerprint,
    attribute_key,
    attribute_value,
    attribute_type,
    attribute_count
FROM
(
    SELECT
        team_id AS team_id,
        toStartOfInterval(timestamp, toIntervalMinute(10)) AS time_bucket,
        service_name AS service_name,
        resource_fingerprint,
        mapFilter((k, v) -> ((length(k) < 256) AND (length(v) < 256)), attributes) AS attributes,
        arrayJoin(attributes) AS attribute,
        'metric' AS attribute_type,
        attribute.1 AS attribute_key,
        attribute.2 AS attribute_value,
        sumSimpleState(1) AS attribute_count
    FROM metrics1
    GROUP BY
        team_id,
        time_bucket,
        service_name,
        resource_fingerprint,
        attributes
);

CREATE MATERIALIZED VIEW metrics1_to_resource_attributes TO metric_attributes
(
    team_id Int32,
    time_bucket DateTime64(0, 'UTC'),
    service_name LowCardinality(String),
    resource_fingerprint UInt64,
    attribute_key LowCardinality(String),
    attribute_value String,
    attribute_type LowCardinality(String),
    attribute_count SimpleAggregateFunction(sum, UInt64)
)
AS SELECT
    team_id,
    time_bucket,
    service_name,
    resource_fingerprint,
    attribute_key,
    attribute_value,
    attribute_type,
    attribute_count
FROM
(
    SELECT
        team_id AS team_id,
        toStartOfInterval(timestamp, toIntervalMinute(10)) AS time_bucket,
        service_name AS service_name,
        resource_fingerprint,
        arrayJoin(resource_attributes) AS attribute,
        'resource' AS attribute_type,
        attribute.1 AS attribute_key,
        attribute.2 AS attribute_value,
        sumSimpleState(1) AS attribute_count
    FROM metrics1
    GROUP BY
        team_id,
        time_bucket,
        service_name,
        resource_fingerprint,
        resource_attributes
);
