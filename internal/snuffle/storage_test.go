package snuffle

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

func TestSampleIteratorSeek(t *testing.T) {
	it := &sampleIterator{
		idx: -1,
		points: []seriesPoint{
			{t: 10, f: 1, typ: chunkenc.ValFloat},
			{t: 20, f: 2, typ: chunkenc.ValFloat},
			{t: 30, f: 3, typ: chunkenc.ValFloat},
		},
	}

	if got := it.Seek(20); got != chunkenc.ValFloat {
		t.Fatalf("Seek returned %s", got)
	}
	ts, value := it.At()
	if ts != 20 || value != 2 {
		t.Fatalf("At after seek = (%d, %v), want (20, 2)", ts, value)
	}
	if got := it.Seek(15); got != chunkenc.ValFloat {
		t.Fatalf("Seek before current returned %s", got)
	}
	ts, value = it.At()
	if ts != 20 || value != 2 {
		t.Fatalf("Seek moved backwards to (%d, %v)", ts, value)
	}
	if got := it.Next(); got != chunkenc.ValFloat {
		t.Fatalf("Next returned %s", got)
	}
	ts, value = it.At()
	if ts != 30 || value != 3 {
		t.Fatalf("At after next = (%d, %v), want (30, 3)", ts, value)
	}
}

func TestParseLabelsJSONAcceptsObjectAndEncodedString(t *testing.T) {
	for _, raw := range []string{
		`{"job":"api","instance":"host-1"}`,
		`"{\"job\":\"api\",\"instance\":\"host-1\"}"`,
	} {
		labelsMap, err := parseLabelsJSON(json.RawMessage(raw))
		if err != nil {
			t.Fatalf("parseLabelsJSON(%s) returned error: %v", raw, err)
		}
		if labelsMap["job"] != "api" || labelsMap["instance"] != "host-1" {
			t.Fatalf("parseLabelsJSON(%s) = %#v", raw, labelsMap)
		}
	}
}

func TestSortedLimitedSortsLabelValues(t *testing.T) {
	values := map[string]struct{}{
		"worker": {},
		"api":    {},
		"db":     {},
	}

	got := sortedLimited(values, 0)
	want := []string{"api", "db", "worker"}
	if len(got) != len(want) {
		t.Fatalf("sortedLimited length = %d, want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sortedLimited[%d] = %q, want %q; full result %#v", i, got[i], want[i], got)
		}
	}
}

func TestSeriesSetFromMetaSortsByLabels(t *testing.T) {
	workerLabels := labels.FromStrings(labels.MetricName, "up", "type", "worker")
	apiLabels := labels.FromStrings(labels.MetricName, "up", "type", "api")
	dbLabels := labels.FromStrings(labels.MetricName, "up", "type", "db")

	set := seriesSetFromMeta([]*seriesMeta{
		{id: 1, labels: workerLabels},
		{id: 2, labels: apiLabels},
		{id: 3, labels: dbLabels},
	}, true)

	var got []string
	for set.Next() {
		got = append(got, set.At().Labels().Get("type"))
	}
	want := []string{"api", "db", "worker"}
	if len(got) != len(want) {
		t.Fatalf("series count = %d, want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("series order = %#v, want %#v", got, want)
		}
	}
}

func TestShouldSortSeriesForInstantQueries(t *testing.T) {
	if !shouldSortSeries(false, &storage.SelectHints{Step: 0}) {
		t.Fatal("instant query series should be sorted")
	}
	if shouldSortSeries(false, &storage.SelectHints{Step: 60_000}) {
		t.Fatal("range query series should not be sorted before evaluation")
	}
	if !shouldSortSeries(true, &storage.SelectHints{Step: 60_000}) {
		t.Fatal("explicitly requested series sort should be respected")
	}
}

func TestFutureSeriesSetWaitsForSelection(t *testing.T) {
	future := &selectFuture{done: make(chan struct{})}
	set := &futureSeriesSet{future: future}
	next := make(chan bool, 1)
	go func() { next <- set.Next() }()
	select {
	case <-next:
		t.Fatal("Next returned before selection completed")
	default:
	}
	future.series = []*seriesMeta{{labels: labels.FromStrings(labels.MetricName, "up")}}
	close(future.done)
	if !<-next || set.At().Labels().Get(labels.MetricName) != "up" {
		t.Fatal("future series was not returned")
	}
}

func TestSelectCacheKeyIncludesHintsAndMatchers(t *testing.T) {
	hints := &storage.SelectHints{Start: 1000, End: 2000, Step: 100, Func: "sum", Grouping: []string{"instance"}, By: true}
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "up"),
		labels.MustNewMatcher(labels.MatchEqual, "job", "api"),
	}
	key := selectCacheKey(false, hints, matchers)
	if got := selectCacheKey(false, hints, matchers); got != key {
		t.Fatalf("identical select key = %q, want %q", got, key)
	}
	changedHints := *hints
	changedHints.Start++
	if got := selectCacheKey(false, &changedHints, matchers); got == key {
		t.Fatal("different select hints produced the same cache key")
	}
	changedMatchers := append([]*labels.Matcher{}, matchers...)
	changedMatchers[1] = labels.MustNewMatcher(labels.MatchEqual, "job", "worker")
	if got := selectCacheKey(false, hints, changedMatchers); got == key {
		t.Fatal("different matchers produced the same cache key")
	}
}

func TestMatchersUnsatisfiableIntersectsFiniteMatcherValues(t *testing.T) {
	if !matchersUnsatisfiable([]*labels.Matcher{
		labels.MustNewMatcher(labels.MatchRegexp, "type", "ingestion-.*"),
		labels.MustNewMatcher(labels.MatchRegexp, "type", "offline|online"),
	}) {
		t.Fatal("disjoint type matchers should be unsatisfiable")
	}
	if matchersUnsatisfiable([]*labels.Matcher{
		labels.MustNewMatcher(labels.MatchRegexp, "type", "off.*"),
		labels.MustNewMatcher(labels.MatchRegexp, "type", "offline|online"),
	}) {
		t.Fatal("overlapping type matchers should be satisfiable")
	}
}

func TestLatestSamplesSQLFromMatchers(t *testing.T) {
	cfg := Config{
		CHDatabase:      "default",
		SeriesTable:     "series",
		SamplesTable:    "samples",
		LabelIndexTable: "label_index",
	}
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "http_requests_total"),
		labels.MustNewMatcher(labels.MatchEqual, "job", "api"),
	}

	sql, ok := samplesSQLFromMatchers(cfg, matchers, 1000, 2000, true)
	if !ok {
		t.Fatal("samplesSQLFromMatchers returned ok=false")
	}
	for _, want := range []string{
		"argMax(value, timestamp)",
		"SELECT id, timestamp, value",
		"`default`.`samples`",
		"`default`.`label_index`",
		"team_id = 0",
		"metric_name = 'http_requests_total'",
		"toStartOfTenMinutes(timestamp) >= toStartOfTenMinutes(fromUnixTimestamp64Milli(1000, 'UTC'))",
		"toStartOfTenMinutes(timestamp) <= toStartOfTenMinutes(fromUnixTimestamp64Milli(2000, 'UTC'))",
		"label_name = 'job'",
		"label_value = 'api'",
		nonStaleSampleSQL("value"),
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL %q does not contain %q", sql, want)
		}
	}
}

func TestLatestSamplesSQLPushesSafeNegativeMatcher(t *testing.T) {
	cfg := Config{
		CHDatabase:      "default",
		SeriesTable:     "series",
		SamplesTable:    "samples",
		LabelIndexTable: "label_index",
	}
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchNotEqual, "status", "500"),
	}
	sql, ok := samplesSQLFromMatchers(cfg, matchers, 1000, 2000, true)
	if !ok {
		t.Fatal("negative matcher that matches missing labels should push down through NOT IN")
	}
	for _, want := range []string{
		"`default`.`label_index`",
		"id NOT IN",
		"label_name = 'status'",
		"label_value = '500'",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL %q does not contain %q", sql, want)
		}
	}
}

func TestLatestSamplesSQLRejectsNegativeMatcherThatDoesNotMatchMissingLabels(t *testing.T) {
	cfg := Config{
		CHDatabase:      "default",
		SeriesTable:     "series",
		SamplesTable:    "samples",
		LabelIndexTable: "label_index",
	}
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchNotEqual, "status", ""),
	}
	if _, ok := samplesSQLFromMatchers(cfg, matchers, 1000, 2000, true); ok {
		t.Fatal("negative matcher that excludes missing labels should fall back to exact selected IDs")
	}
}

func TestRangeSamplesSQLFromMatchers(t *testing.T) {
	cfg := Config{
		CHDatabase:      "default",
		SeriesTable:     "series",
		SamplesTable:    "samples",
		LabelIndexTable: "label_index",
	}
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "http_requests_total"),
		labels.MustNewMatcher(labels.MatchEqual, "job", "api"),
	}

	sql, ok := samplesSQLFromMatchers(cfg, matchers, 1000, 2000, false)
	if !ok {
		t.Fatal("samplesSQLFromMatchers returned ok=false")
	}
	for _, want := range []string{
		"toUnixTimestamp64Milli(timestamp)",
		"SELECT id, timestamp, value",
		"ORDER BY id, timestamp",
		"`default`.`samples`",
		"`default`.`label_index`",
		"team_id = 0",
		"metric_name = 'http_requests_total'",
		"label_name = 'job'",
		"label_value = 'api'",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL %q does not contain %q", sql, want)
		}
	}
}

func TestRangeSamplesSQLIgnoresNoopLabelMatcher(t *testing.T) {
	cfg := Config{
		CHDatabase:      "default",
		SeriesTable:     "series",
		SamplesTable:    "samples",
		LabelIndexTable: "label_index",
	}
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "node_cpu_seconds_total"),
		labels.MustNewMatcher(labels.MatchRegexp, "type", ".*"),
		labels.MustNewMatcher(labels.MatchEqual, "ready", "true"),
	}

	sql, ok := samplesSQLFromMatchers(cfg, matchers, 1000, 2000, false)
	if !ok {
		t.Fatal("samplesSQLFromMatchers returned ok=false")
	}
	if strings.Contains(sql, "label_name = 'type'") {
		t.Fatalf("SQL %q should not push noop matcher", sql)
	}
	for _, want := range []string{
		"metric_name = 'node_cpu_seconds_total'",
		"label_name = 'ready'",
		"label_value = 'true'",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL %q does not contain %q", sql, want)
		}
	}
}

func TestSamplesSQLKeepsMetricConstraintForIDBatches(t *testing.T) {
	cfg := Config{
		CHDatabase:   "default",
		SamplesTable: "samples",
	}
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "http_requests_total"),
		labels.MustNewMatcher(labels.MatchEqual, "status", "200"),
	}

	sql := samplesSQL(cfg, []uint64{1, 2}, 1000, 2000, false, matchers)
	for _, want := range []string{
		"`default`.`samples`",
		"team_id = 0",
		"id IN (1,2)",
		"timestamp >= fromUnixTimestamp64Milli(1000, 'UTC')",
		"timestamp <= fromUnixTimestamp64Milli(2000, 'UTC')",
		"metric_name = 'http_requests_total'",
		"ORDER BY id, timestamp",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL %q does not contain %q", sql, want)
		}
	}
	if strings.Contains(sql, "label_name = 'status'") {
		t.Fatalf("small id-batch sample SQL should not redo label-index filtering: %s", sql)
	}
}

func TestPostHogSeriesSamplesSQLUsesPostHogTablesAndAttributePredicates(t *testing.T) {
	cfg := Config{
		CHDatabase:   "posthog",
		SchemaLayout: "posthog",
		SamplesTable: "metrics1",
	}
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "http_requests_total"),
		labels.MustNewMatcher(labels.MatchEqual, "service_name", "checkout"),
		labels.MustNewMatcher(labels.MatchEqual, "status", "200"),
	}

	sql := postHogSeriesSamplesSQL(cfg, matchers, 1000, 2000, false)
	for _, want := range []string{
		"`posthog`.`metrics1`",
		"xxHash64(metric_name, service_name, resource_fingerprint, mapSort(attributes_map_str)) AS series_id",
		"time_bucket >= toStartOfDay(fromUnixTimestamp64Milli(1000, 'UTC'))",
		"time_bucket <= toStartOfDay(fromUnixTimestamp64Milli(2000, 'UTC'))",
		"service_name = 'checkout'",
		"metric_name = 'http_requests_total'",
		"if(mapContains(attributes_map_str, 'status__str'), attributes_map_str['status__str'], resource_attributes['status']) = '200'",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL %q does not contain %q", sql, want)
		}
	}
	for _, notWant := range []string{"metrics_series", "metrics_label_index", "id IN"} {
		if strings.Contains(sql, notWant) {
			t.Fatalf("posthog SQL should not use %q:\n%s", notWant, sql)
		}
	}
}

func TestPostHogLoadSamplesSQLFiltersByComputedSeriesID(t *testing.T) {
	cfg := Config{
		CHDatabase:   "posthog",
		SchemaLayout: "posthog",
		SamplesTable: "metrics1",
	}
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "http_requests_total"),
	}

	sql := postHogLoadSamplesSQL(cfg, []uint64{1, 2}, matchers, 1000, 2000, true)
	for _, want := range []string{
		"`posthog`.`metrics1`",
		"time_bucket >= toStartOfDay(fromUnixTimestamp64Milli(1000, 'UTC'))",
		"xxHash64(metric_name, service_name, resource_fingerprint, mapSort(attributes_map_str)) IN (1,2)",
		"argMax(value, timestamp)",
		nonStaleSampleSQL("value"),
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL %q does not contain %q", sql, want)
		}
	}
}

func TestTopKSelectedSeriesCarriesLabelsAndMetricConstraint(t *testing.T) {
	cfg := Config{
		CHDatabase:      "default",
		SeriesTable:     "series",
		LabelIndexTable: "label_index",
	}
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "http_requests_total"),
		labels.MustNewMatcher(labels.MatchEqual, "status", "200"),
	}

	sql, ok := selectedSeriesSQL(cfg, matchers, 1000, 2000, []string{"id", "metric_name", "labels_json"})
	if !ok {
		t.Fatal("selectedSeriesSQL returned ok=false")
	}
	for _, want := range []string{
		"`default`.`series`",
		"`default`.`label_index`",
		"any(metric_name) AS metric_name",
		"any(labels_json) AS labels_json",
		"team_id = 0",
		"metric_name = 'http_requests_total'",
		"label_name = 'status'",
		"label_value = '200'",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL %q does not contain %q", sql, want)
		}
	}
}

func TestSamplesForSelectedSeriesSQLPrunesViaLabelIndex(t *testing.T) {
	cfg := Config{CHDatabase: "default", SeriesTable: "series", SamplesTable: "samples", LabelIndexTable: "label_index"}
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "http_requests_total"),
		labels.MustNewMatcher(labels.MatchEqual, "status", "200"),
	}

	sql := samplesForSelectedSeriesSQL(cfg, matchers, 1000, 2000)
	// Naming selected_series here would make ClickHouse run the series lookup a
	// second time, since it inlines CTEs instead of materialising them.
	if strings.Contains(sql, "selected_series") {
		t.Fatalf("sample scan references the selected_series CTE:\n%s", sql)
	}
	for _, want := range []string{
		"`default`.`label_index`",
		"metric_name = 'http_requests_total'",
		"label_name = 'status'",
		"label_value = '200'",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}

	// A matcher the label index cannot answer has to fall back to the CTE.
	unindexable := []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "status", "2.*")}
	if sql := samplesForSelectedSeriesSQL(cfg, unindexable, 1000, 2000); !strings.Contains(sql, "selected_series") {
		t.Fatalf("unindexable matcher did not fall back to selected_series:\n%s", sql)
	}
}
