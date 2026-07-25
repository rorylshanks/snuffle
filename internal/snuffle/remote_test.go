package snuffle

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	promvalue "github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/prompb"
)

func TestBuildRemoteWriteBatch(t *testing.T) {
	req := &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{
		{
			Labels: []prompb.Label{
				{Name: labels.MetricName, Value: "http_requests_total"},
				{Name: "job", Value: "api"},
				{Name: "instance", Value: "host-1"},
			},
			Samples: []prompb.Sample{
				{Timestamp: 1000, Value: 1},
				{Timestamp: 2000, Value: 2},
			},
			Exemplars: []prompb.Exemplar{{
				Labels:    []prompb.Label{{Name: "trace_id", Value: "abc"}},
				Value:     2,
				Timestamp: 2000,
			}},
		},
	}, Metadata: []prompb.MetricMetadata{{
		MetricFamilyName: "http_requests",
		Type:             prompb.MetricMetadata_COUNTER,
		Help:             "request count",
		Unit:             "requests",
	}}}

	batch, err := buildRemoteWriteBatch(req, 0, 7, false)
	if err != nil {
		t.Fatalf("buildRemoteWriteBatch returned error: %v", err)
	}
	if batch.seriesCount != 1 || batch.sampleCount != 2 || batch.exemplarCount != 1 || batch.metadataCount != 1 {
		t.Fatalf("counts = series %d samples %d exemplars %d metadata %d", batch.seriesCount, batch.sampleCount, batch.exemplarCount, batch.metadataCount)
	}
	if len(batch.seriesRecords) != 1 {
		t.Fatalf("seriesRecords length = %d, want 1", len(batch.seriesRecords))
	}
	if err := populateSeriesLabelsJSON(batch.seriesRecords); err != nil {
		t.Fatalf("populate labels json: %v", err)
	}
	seriesRow := batch.seriesRecords[0]
	if seriesRow.TeamID != 7 || seriesRow.MetricName != "http_requests_total" || seriesRow.MinMS != 1000 || seriesRow.MaxMS != 2000 || !strings.HasPrefix(seriesRow.LabelsJSON, "{") {
		t.Fatalf("unexpected series row: %#v", seriesRow)
	}
	if len(batch.exemplarRows) != 1 || !strings.Contains(batch.exemplarRows[0].LabelsJSON, `"trace_id":"abc"`) {
		t.Fatalf("exemplar rows should contain exemplar labels: %#v", batch.exemplarRows)
	}
	if len(batch.metadataRows) != 1 || batch.metadataRows[0].Type != "counter" {
		t.Fatalf("metadata rows should contain metadata: %#v", batch.metadataRows)
	}
}

func TestBuildRemoteWriteBatchPreservesStaleNaNSamples(t *testing.T) {
	stale := math.Float64frombits(promvalue.StaleNaN)
	req := &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{
		{
			Labels: []prompb.Label{{Name: labels.MetricName, Value: "up"}, {Name: "job", Value: "api"}},
			Samples: []prompb.Sample{
				{Timestamp: 1_000, Value: math.NaN()},
				{Timestamp: 2_000, Value: 2},
				{Timestamp: 3_000, Value: stale},
			},
		},
		{
			Labels: []prompb.Label{{Name: labels.MetricName, Value: "up"}, {Name: "job", Value: "gone"}},
			Samples: []prompb.Sample{
				{Timestamp: 4_000, Value: stale},
			},
		},
		{
			Labels: []prompb.Label{{Name: labels.MetricName, Value: "up"}, {Name: "job", Value: "noise"}},
			Samples: []prompb.Sample{
				{Timestamp: 5_000, Value: math.NaN()},
			},
		},
	}}

	batch, err := buildRemoteWriteBatch(req, 0, 0, false)
	if err != nil {
		t.Fatalf("buildRemoteWriteBatch returned error: %v", err)
	}
	if batch.seriesCount != 2 || batch.sampleCount != 3 {
		t.Fatalf("counts = series %d samples %d", batch.seriesCount, batch.sampleCount)
	}
	if len(batch.sampleRows) != 3 || math.IsNaN(batch.sampleRows[0].Value) || batch.sampleRows[0].Value != 2 || batch.sampleRows[0].TimestampMS != 2000 {
		t.Fatalf("unexpected sample rows: %#v", batch.sampleRows)
	}
	if !isStaleSampleValue(batch.sampleRows[1].Value) || batch.sampleRows[1].TimestampMS != 3000 {
		t.Fatalf("second sample should be stale at 3000ms: %#v", batch.sampleRows)
	}
	if !isStaleSampleValue(batch.sampleRows[2].Value) || batch.sampleRows[2].TimestampMS != 4000 {
		t.Fatalf("third sample should be stale at 4000ms: %#v", batch.sampleRows)
	}
	if batch.sampleRows[0].MetricName != "up" || batch.sampleRows[1].MetricName != "up" || batch.sampleRows[2].MetricName != "up" {
		t.Fatalf("sample rows should carry metric name for inserts: %#v", batch.sampleRows)
	}
	if len(batch.seriesRecords) != 2 || batch.seriesRecords[0].MinMS != 2000 || batch.seriesRecords[0].MaxMS != 3000 || batch.seriesRecords[1].MinMS != 4000 || batch.seriesRecords[1].MaxMS != 4000 {
		t.Fatalf("unexpected series records: %#v", batch.seriesRecords)
	}
}

func TestBuildRemoteWriteBatchAcceptsNativeHistograms(t *testing.T) {
	req := &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{{
		Labels: []prompb.Label{{Name: labels.MetricName, Value: "latency"}},
		Histograms: []prompb.Histogram{{
			Count:     &prompb.Histogram_CountInt{CountInt: 1},
			Sum:       1,
			ZeroCount: &prompb.Histogram_ZeroCountInt{ZeroCountInt: 1},
			Timestamp: 1000,
		}},
	}}}
	batch, err := buildRemoteWriteBatch(req, 0, 0, false)
	if err != nil {
		t.Fatalf("buildRemoteWriteBatch returned error: %v", err)
	}
	if batch.seriesCount != 1 || batch.histogramCount != 1 || batch.sampleCount != 0 {
		t.Fatalf("counts = series %d histograms %d samples %d", batch.seriesCount, batch.histogramCount, batch.sampleCount)
	}
	if len(batch.histogramRows) != 1 || len(batch.histogramRows[0].Histogram) == 0 {
		t.Fatalf("unexpected histogram rows: %#v", batch.histogramRows)
	}
	if batch.histogramRows[0].MetricName != "latency" {
		t.Fatalf("histogram rows should carry metric name for activity MVs: %#v", batch.histogramRows)
	}
}

func TestBuildRemoteWriteBatchBucketsSamples(t *testing.T) {
	req := &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{{
		Labels: []prompb.Label{{Name: labels.MetricName, Value: "up"}, {Name: "job", Value: "api"}},
		Samples: []prompb.Sample{
			{Timestamp: 1_000, Value: 1},
			{Timestamp: 14_999, Value: 2},
			{Timestamp: 15_000, Value: 3},
		},
		Histograms: []prompb.Histogram{
			{
				Count:     &prompb.Histogram_CountInt{CountInt: 1},
				Sum:       1,
				ZeroCount: &prompb.Histogram_ZeroCountInt{ZeroCountInt: 1},
				Timestamp: 14_500,
			},
			{
				Count:     &prompb.Histogram_CountInt{CountInt: 2},
				Sum:       2,
				ZeroCount: &prompb.Histogram_ZeroCountInt{ZeroCountInt: 1},
				Timestamp: 14_900,
			},
		},
	}}}

	batch, err := buildRemoteWriteBatch(req, 15*time.Second, 0, false)
	if err != nil {
		t.Fatalf("buildRemoteWriteBatch returned error: %v", err)
	}
	if batch.sampleCount != 2 {
		t.Fatalf("sampleCount = %d, want 2", batch.sampleCount)
	}
	if batch.histogramCount != 2 {
		t.Fatalf("histogramCount = %d, want 2", batch.histogramCount)
	}
	if len(batch.sampleRows) != 2 ||
		batch.sampleRows[0].TimestampMS != 0 ||
		batch.sampleRows[0].Value != 2 ||
		batch.sampleRows[1].TimestampMS != 15000 ||
		batch.sampleRows[1].Value != 3 {
		t.Fatalf("unexpected sample rows: %#v", batch.sampleRows)
	}
	if len(batch.histogramRows) != 2 || batch.histogramRows[0].TimestampMS != 0 || batch.histogramRows[0].Version != 14500 || batch.histogramRows[1].TimestampMS != 0 || batch.histogramRows[1].Version != 14900 {
		t.Fatalf("unexpected histogram rows: %#v", batch.histogramRows)
	}
}

func TestBuildRemoteWriteBatchFromProtoFastPath(t *testing.T) {
	stale := math.Float64frombits(promvalue.StaleNaN)
	req := &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{{
		Labels: []prompb.Label{
			{Name: labels.MetricName, Value: "up"},
			{Name: "instance", Value: "host-1"},
			{Name: "job", Value: "api"},
		},
		Samples: []prompb.Sample{
			{Timestamp: 1_000, Value: 1},
			{Timestamp: 14_000, Value: 3},
			{Timestamp: 16_000, Value: 2},
			{Timestamp: 17_000, Value: math.NaN()},
			{Timestamp: 31_000, Value: stale},
		},
	}}}
	payload, err := req.Marshal()
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	batch, ok, err := buildRemoteWriteBatchFromProto(payload, 15*time.Second, 7)
	if err != nil {
		t.Fatalf("buildRemoteWriteBatchFromProto returned error: %v", err)
	}
	if !ok {
		t.Fatal("buildRemoteWriteBatchFromProto did not use fast path")
	}
	if batch.seriesCount != 1 || batch.sampleCount != 3 || batch.histogramCount != 0 || batch.exemplarCount != 0 || batch.metadataCount != 0 {
		t.Fatalf("counts = series %d samples %d histograms %d exemplars %d metadata %d", batch.seriesCount, batch.sampleCount, batch.histogramCount, batch.exemplarCount, batch.metadataCount)
	}
	if got := batch.sampleColumns; got.TeamIDs[0] != 7 || got.MetricNames[0] != "up" || got.Timestamps[0] != 0 || got.Values[0] != 3 {
		t.Fatalf("unexpected first sample columns: %#v", got)
	}
	if got := batch.sampleColumns; got.Timestamps[1] != 15000 || got.Values[1] != 2 {
		t.Fatalf("unexpected second sample columns: %#v", got)
	}
	if got := batch.sampleColumns; got.Timestamps[2] != 30000 || !isStaleSampleValue(got.Values[2]) {
		t.Fatalf("unexpected third sample columns: %#v", got)
	}
	if got := batch.seriesRecords[0]; got.TeamID != 7 || got.MetricName != "up" || got.MinMS != 0 || got.MaxMS != 30000 || len(got.Labels) != 0 || got.LabelsJSON != "" {
		t.Fatalf("unexpected series row: %#v", got)
	}
	if err := populateSeriesLabelsJSONFromProto(batch.fastProto, batch.seriesRecords); err != nil {
		t.Fatalf("populate fast labels json: %v", err)
	}
	if got := batch.seriesRecords[0].LabelsJSON; !strings.Contains(got, `"instance":"host-1"`) || !strings.Contains(got, `"job":"api"`) || strings.Contains(got, "__name__") {
		t.Fatalf("unexpected fast labels json: %s", got)
	}
}

func TestBuildRemoteWriteBatchFromProtoFallsBackForComplexMessages(t *testing.T) {
	for name, req := range map[string]*prompb.WriteRequest{
		"metadata": {
			Metadata: []prompb.MetricMetadata{{MetricFamilyName: "up", Type: prompb.MetricMetadata_GAUGE}},
		},
		"histogram": {
			Timeseries: []prompb.TimeSeries{{
				Labels: []prompb.Label{{Name: labels.MetricName, Value: "latency"}},
				Histograms: []prompb.Histogram{{
					Count:     &prompb.Histogram_CountInt{CountInt: 1},
					Sum:       1,
					ZeroCount: &prompb.Histogram_ZeroCountInt{ZeroCountInt: 1},
					Timestamp: 1000,
				}},
			}},
		},
		"exemplar": {
			Timeseries: []prompb.TimeSeries{{
				Labels:    []prompb.Label{{Name: labels.MetricName, Value: "up"}},
				Exemplars: []prompb.Exemplar{{Value: 1, Timestamp: 1000}},
			}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			payload, err := req.Marshal()
			if err != nil {
				t.Fatalf("marshal request: %v", err)
			}
			if _, ok, err := buildRemoteWriteBatchFromProto(payload, 15*time.Second, 0); err != nil || ok {
				t.Fatalf("buildRemoteWriteBatchFromProto = ok %v err %v, want fallback without error", ok, err)
			}
		})
	}
}

func TestRemoteWritePhaseErrorIncludesUsefulContext(t *testing.T) {
	err := remoteWritePhaseError(
		"insert samples",
		"metrics_samples",
		123,
		30*time.Second,
		"series=1 samples=123 histograms=0 exemplars=0 metadata=0",
		time.Now().Add(-1500*time.Millisecond),
		context.DeadlineExceeded,
	)
	got := err.Error()
	for _, want := range []string{
		"remote write insert samples timed out",
		"clickhouse_timeout=30s",
		"table=metrics_samples",
		"rows=123",
		"batch=series=1 samples=123 histograms=0 exemplars=0 metadata=0",
		"context deadline exceeded",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("error %q does not contain %q", got, want)
		}
	}
}

func TestKnownSeriesIDsSQLDrivesLookupFromTheBatch(t *testing.T) {
	cfg := Config{
		CHDatabase:  "default",
		SeriesTable: "series",
		TeamID:      42,
	}
	sql := knownSeriesIDsSQL(cfg, "remote_write_series_ids")
	for _, want := range []string{
		"SELECT id",
		"`default`.`series`",
		"team_id = 42",
		"id IN (SELECT id FROM `remote_write_series_ids`)",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL %q does not contain %q", sql, want)
		}
	}
	// Scanning the tenant's whole series set to answer a question about one
	// batch is what made this cost 2.9s per remote-write request.
	if strings.Contains(sql, "NOT IN") {
		t.Fatalf("SQL still scans the full series set: %q", sql)
	}
}

func TestStableSeriesID(t *testing.T) {
	first := labels.FromMap(map[string]string{
		labels.MetricName: "up",
		"job":             "api",
		"instance":        "host-1",
	})
	second := labels.FromMap(map[string]string{
		"instance":        "host-1",
		labels.MetricName: "up",
		"job":             "api",
	})
	different := labels.FromMap(map[string]string{
		labels.MetricName: "up",
		"job":             "api",
		"instance":        "host-2",
	})

	if stableSeriesID(first) != stableSeriesID(second) {
		t.Fatal("series fingerprint should be independent of input label order")
	}
	if stableSeriesID(first) == stableSeriesID(different) {
		t.Fatal("series fingerprint should change when labels change")
	}
	got, metricName, err := remoteWriteSeriesIdentity([]prompb.Label{
		{Name: "instance", Value: "host-1"},
		{Name: labels.MetricName, Value: "up"},
		{Name: "job", Value: "api"},
	})
	if err != nil {
		t.Fatalf("remoteWriteSeriesIdentity returned error: %v", err)
	}
	if metricName != "up" || got != stableSeriesID(first) {
		t.Fatalf("remoteWriteSeriesIdentity = (%d, %q), want (%d, up)", got, metricName, stableSeriesID(first))
	}
}

func TestRemoteReadMatcherConversion(t *testing.T) {
	matchers, err := remoteReadMatchers([]*prompb.LabelMatcher{
		{Type: prompb.LabelMatcher_EQ, Name: labels.MetricName, Value: "up"},
		{Type: prompb.LabelMatcher_NRE, Name: "job", Value: "debug.*"},
	})
	if err != nil {
		t.Fatalf("remoteReadMatchers returned error: %v", err)
	}
	if len(matchers) != 2 || matchers[0].Type != labels.MatchEqual || matchers[1].Type != labels.MatchNotRegexp {
		t.Fatalf("unexpected matchers: %#v", matchers)
	}
}

func TestRemoteReadAcceptsSamples(t *testing.T) {
	if !remoteReadAcceptsSamples(&prompb.ReadRequest{}) {
		t.Fatal("empty accepted response types should default to samples")
	}
	if !remoteReadAcceptsSamples(&prompb.ReadRequest{AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_SAMPLES}}) {
		t.Fatal("samples response type should be accepted")
	}
	if remoteReadAcceptsSamples(&prompb.ReadRequest{AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_STREAMED_XOR_CHUNKS}}) {
		t.Fatal("chunk-only remote read should not be accepted")
	}
}
